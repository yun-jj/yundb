#ifndef TABLE_FORMAT_H
#define TABLE_FORMAT_H

#include <cstdint>
#include <string>

#include "yundb/slice.h"

namespace yundb
{

// TableMagicNumber was picked by running
//    echo yundb | sha1sum
// and taking the leading 64 bits.
constexpr uint64_t TableMagicNumber = 0xbf920e1798aff023ull;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle
{
 public:
  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle() : _is_decode(false) {}
  // The position of the block in the file.
  uint64_t getPosition() const
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "BlockHadle: no decode get 0 position",
      !_is_decode
    );
    return _position;
  }
  // The size of the stored block
  uint64_t getSize() const
  {
    CERR_PRINT_WITH_CONDITIONAL(
      "BlockHadle: no decode get 0 position",
      !_is_decode
    );
    return _size;
  }
  // Encode position and size to dst
  std::string encode(uint64_t position, uint64_t size) const;
  // Decode position and size
  void decodeFrom(Slice* input);

 private:
  uint64_t _position;
  uint64_t _size;
  bool _is_decode;
};

class Footer
{
 public:
  // Max footer Size
  enum {MaxFooterSize = BlockHandle::kMaxEncodedLength * 2 + 8};

  Footer(){}
  ~Footer(){}

  void setMetaIndexBlockHandle(std::string handle)
  {_mete_index_block_handle = handle;}

  void setIndexBlockHandle(std::string handle)
  {_index_block_handle = handle;}

  void encodeTo(std::string* input);

 private:
  std::string _mete_index_block_handle;
  std::string _index_block_handle;
};


};

#endif