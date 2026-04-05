#include "log_writer.h"
#include "log_format.h"
#include "yundb/slice.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace yundb
{

namespace log
{

void Writer::initTypeCrc()
{
  for (int i = 0; i <= maxRecordType; i++)
  {
    char type = static_cast<char>(i);
    _type_crc[i] = crc32c::Value(&type, 1);
  }
}

void Writer::appendRecord(const Slice& record)
{
  const char* data = record.data();
  size_t write_size = record.size();

  bool begin = true;
  while (write_size > 0)
  {
    size_t available_block_size =  recordBlockSize - _block_offset;
    size_t fragment_size = 
      (available_block_size < write_size) ? available_block_size : write_size; 

    /* need a new block to storage */
    if (fragment_size <= recordHeadSize)
    {
      _dest->append(Slice("\x00\x00\x00", fragment_size));
      _block_offset = 0;
      available_block_size = recordBlockSize - _block_offset;
      fragment_size = (available_block_size < write_size) ? available_block_size : write_size;
    }

    bool end = (write_size == fragment_size);
    RecordType type;
    
    if (begin && end)
      type = FullType;
    else if (begin)
      type = FirstType;
    else if (end)
      type = LastType;
    else
      type = MiddleType;
    
    emitPhysicalRecord(data, type, fragment_size);

    _block_offset += (recordHeadSize + fragment_size);
    data += fragment_size;
    write_size -= fragment_size;
    begin = false;
  }
}

void Writer::emitPhysicalRecord(const char* data, RecordType type, size_t length)
{
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(_block_offset + recordHeadSize + length <= recordBlockSize);

  // Format the header, start 4 bytes for crc
  char buf[recordHeadSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(type);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(_type_crc[type], data, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  _dest->append(Slice(buf, recordHeadSize));
  _dest->append(Slice(data, length));
  _dest->flush();
  _block_offset += recordHeadSize + length;
}

}

}
