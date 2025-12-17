#include "table_format.h"
#include "util/coding.h"
#include "util/error_print.h"


namespace yundb
{

std::string BlockHandle::encode(uint64_t position, uint64_t size) const
{
  // Sanity check that all fields have been set
  CERR_PRINT_WITH_CONDITIONAL(
    "BlockHandle::Encode: error position value",
    position == ~static_cast<uint64_t>(0)
  );
  CERR_PRINT_WITH_CONDITIONAL(
    "BlockHandle::Encode: error size value",
    size == ~static_cast<uint64_t>(0)
  );
  std::string result;
  PutVarint64(&result, _position);
  PutVarint64(&result, _size);
  return result;
}

void BlockHandle::decodeFrom(Slice* input)
{
  if (GetVarint64(input, &_position) && GetVarint64(input, &_size))
  {
    _is_decode = true;
    return;
  }
  CERR_PRINT("BlockHandle::DecodeFrom: bad encode");
}

void Footer::encodeTo(std::string* dst)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: None handle",
    _mete_index_block_handle.empty() || _index_block_handle.empty()
  );

  CERR_PRINT_WITH_CONDITIONAL(
    "handle to bigger to fill",
    _mete_index_block_handle.size() + _index_block_handle.size() > MaxFooterSize - 8
  );

  CERR_PRINT_WITH_CONDITIONAL("Footer: None dst", dst == nullptr);

  size_t initSize = dst->size();
  dst->append(_mete_index_block_handle);
  dst->append(_index_block_handle);
  // Append '\0'
  dst->resize(MaxFooterSize - 8, '\0');
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber >> 32));

  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: size error",
    dst->size() - initSize != MaxFooterSize
  );

  // Disable unused variable warning.
  (void)initSize;
}

}

