#include "log_writer.h"
#include "log_format.h"
#include "yundb/slice.h"

namespace yundb
{
namespace log
{

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
    
    auto emitPhysicalRecord = [&](const char* data, RecordType type, size_t length){
      char recordHead[recordHeadSize];
      recordHead[0] = static_cast<char>(length & 0xff);
      recordHead[1] = static_cast<char>(length >> 8);
      recordHead[2] = static_cast<char>(type);

      _dest->append(Slice(recordHead, recordHeadSize));
      _dest->append(Slice(data, length));
      _dest->flush();
    };
    emitPhysicalRecord(data, type, fragment_size);

    _block_offset += (recordHeadSize + fragment_size);
    data += fragment_size;
    write_size -= fragment_size;
    begin = false;
  }
}

}
}
