#include "log_format.h"
#include "log_reader.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace yundb
{

namespace log
{

enum {
  Eof = maxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
  BadRecord = maxRecordType + 2
};

Reader::Reader(SequentialFile* file, size_t initialOffset, bool checksum)
    : _file(file),
      _initialOffset(initialOffset),
      _lastRecordOffset(0),
      _endOfBufferOffset(0),
      _data(new char[recordBlockSize]),
      _checksum(checksum),
      _eof(false),
      _resyncing(initialOffset > 0) {}

Reader::~Reader()
{delete[] _data;}

size_t Reader::lastRecordOffset() const
{return _lastRecordOffset;}

bool Reader::readRecord(Slice* record, std::string* scratch)
{
  if (_lastRecordOffset < _initialOffset) skipToInitialBlock();

  scratch->clear();
  record->clear();
  bool inFragmentedRecord = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospectiveRecordOffset = 0;

  Slice fragment;
  while (true)
  {
    const unsigned int recordType = readPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physicalRecordOffset =
        _endOfBufferOffset - _readBuf.size() - recordHeadSize - fragment.size();

    if (_resyncing)
    {
      if (recordType == MiddleType) {
        continue;
      } else if (recordType == LastType) {
        _resyncing = false;
        continue;
      } else {
        _resyncing = false;
      }
    }

    switch (recordType)
    {
      case FullType:
        if (inFragmentedRecord)
        {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty())
            printError("log::Reader: partial record without end(1)");
        }
        prospectiveRecordOffset = physicalRecordOffset;
        scratch->clear();
        *record = fragment;
        _lastRecordOffset = prospectiveRecordOffset;
        return true;

      case FirstType:
        if (inFragmentedRecord)
        {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty())
            printError("log::Reader: partial record without end(2)");
        }
        prospectiveRecordOffset = physicalRecordOffset;
        scratch->assign(fragment.data(), fragment.size());
        inFragmentedRecord = true;
        break;

      case MiddleType:
        if (!inFragmentedRecord) {
          printError("log::Reader: missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case LastType:
        if (!inFragmentedRecord) {
          printError("log::Reader: missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          _lastRecordOffset = prospectiveRecordOffset;
          return true;
        }
        break;

      case Eof:
        if (inFragmentedRecord)
        {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case BadRecord:
        if (inFragmentedRecord)
        {
          printError("log::Reader: error in middle of record");
          inFragmentedRecord = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        printError("log::Reader: unknown record type");
        inFragmentedRecord = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

void Reader::skipToInitialBlock()
{
  const size_t offsetInBlock = _initialOffset % recordBlockSize;
  size_t blockStartLocation = _initialOffset - offsetInBlock;

  // Don't search a block if we'd be in the trailer
  if (offsetInBlock > recordBlockSize - 6)
    blockStartLocation += recordBlockSize;

  _endOfBufferOffset = blockStartLocation;
  if (blockStartLocation > 0)
    _file->skip(blockStartLocation);  
}

unsigned int Reader::readPhysicalRecord(Slice* result)
{
  while (true)
  {
    if (_readBuf.size() < recordHeadSize)
    {
      if (!_eof) {
        // Try to read a new block
        _readBuf.clear();
        bool success = _file->read(&_readBuf, _data, recordBlockSize);
        _endOfBufferOffset += _readBuf.size();

        if (!success) {
          _readBuf.clear();
          _eof = true;
          printError("log::Reader: read file error");
          return Eof;
        } else if (_readBuf.size() < recordBlockSize) {
          _eof = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        _readBuf.clear();
        return Eof;
      }
    }

    // Parse the header
    const char* header = _readBuf.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);

    if (recordHeadSize + length > _readBuf.size())
    {
      _readBuf.clear();
      if (!_eof)
      {
        printError("log::Reader: bad record length");
        return BadRecord;
      }
      return Eof;
    }

    if (type == ZeroType && length == 0)
    {
      _readBuf.clear();
      return BadRecord;
    }

    // Check crc.
    if (_checksum)
    {
      uint32_t expectedCrc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actualCrc = crc32c::Value(header + 6, length + 1);
      if (actualCrc != expectedCrc)
      {
        _readBuf.clear();
        printError("log::Reader: checksum mismatch");
        return BadRecord;
      }
    }

    _readBuf.removePrefix(recordHeadSize + length);

    // Skip physical record that started before initial_offset_
    if (_endOfBufferOffset - _readBuf.size() - recordHeadSize - length <
        _initialOffset)
    {
      result->clear();
      return BadRecord;
    }

    *result = Slice(header + recordHeadSize, length);
    return type;
  }
}

}

}