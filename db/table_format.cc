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

const char* BlockHandle::decodeFrom(const char* data)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "BlockHandle: None ptr",
    data == nullptr
  );

  data =  GetVarint64Ptr(data, data + 10, &_position);
  data = GetVarint64Ptr(data, data + 10, &_size);
  _is_decode = true;

  return data;
}

Footer::Footer(const Slice& footerBlock)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: footer size error",
    footerBlock.size() != MaxFooterSize
  );

  const char* data = footerBlock.data();

  uint32_t flag1, flag2;
  flag1 = DecodeFixed32(data + BlockHandle::kMaxEncodedLength * 2);

  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: flag number error",
    static_cast<uint32_t>(TableMagicNumber & 0xffffffffu) != flag1
  );

  flag2 = DecodeFixed32(data + BlockHandle::kMaxEncodedLength * 2 + 4);
  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: flag number error",
    static_cast<uint32_t>(TableMagicNumber >> 32) != flag2
  );

  data = _metaIndexHandle.decodeFrom(data);
  _indexBlockHandle.decodeFrom(data);
}

void Footer::encodeTo(std::string* dst, const std::string& metaIndexHandle,
                      const std::string& indexBlockHandle)
{
  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: None handle",
    metaIndexHandle.empty() || indexBlockHandle.empty()
  );

  CERR_PRINT_WITH_CONDITIONAL(
    "handle to bigger to fill",
    metaIndexHandle.size() + indexBlockHandle.size() > MaxFooterSize - 8
  );

  CERR_PRINT_WITH_CONDITIONAL("Footer: None dst", dst == nullptr);

  size_t initSize = dst->size();
  dst->append(metaIndexHandle);
  dst->append(indexBlockHandle);
  // Append '\0'
  dst->resize(MaxFooterSize - 8, '\0');
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber >> 32));

  CERR_PRINT_WITH_CONDITIONAL(
    "Footer: size error",
    dst->size() - initSize != MaxFooterSize
  );
}

inline void Footer::getMetaIndexPosAndSize(uint64_t* pos, uint64_t* size)
{
  if (pos != nullptr) *pos = _metaIndexHandle.getPosition();
  if (size != nullptr) *size = _metaIndexHandle.getSize();
}

inline void Footer::getIndexBlockPosAndSize(uint64_t* pos, uint64_t* size)
{
  if (pos != nullptr) *pos = _indexBlockHandle.getPosition();
  if (size != nullptr) *size = _indexBlockHandle.getSize();
}

}

