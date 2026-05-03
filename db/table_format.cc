#include "table_format.h"
#include "util/coding.h"
#include "util/error_print.h"


namespace yundb
{

std::string BlockHandle::encode(uint64_t position, uint64_t size) const
{
  // Sanity check that all fields have been set
  if (position == ~static_cast<uint64_t>(0))
    printError("BlockHandle::Encode: error position value");
  if (size == ~static_cast<uint64_t>(0))
    printError("BlockHandle::Encode: error size value");
  std::string result;
  PutVarint64(&result, _position);
  PutVarint64(&result, _size);
  return result;
}

const char* BlockHandle::decodeFrom(const char* data)
{
  if (data == nullptr)
    printError("BlockHandle: None ptr");

  data =  GetVarint64Ptr(data, data + 10, &_position);
  data = GetVarint64Ptr(data, data + 10, &_size);
  _is_decode = true;

  return data;
}

Footer::Footer(const Slice& footerBlock)
{
  if (footerBlock.size() != MaxFooterSize)
    printError("Footer: footer size error");

  const char* data = footerBlock.data();

  uint32_t flag1, flag2;
  flag1 = DecodeFixed32(data + BlockHandle::kMaxEncodedLength * 2);

  if (static_cast<uint32_t>(TableMagicNumber & 0xffffffffu) != flag1)
    printError("Footer: flag number error");

  flag2 = DecodeFixed32(data + BlockHandle::kMaxEncodedLength * 2 + 4);
  if (static_cast<uint32_t>(TableMagicNumber >> 32) != flag2)
    printError("Footer: flag number error");

  data = _metaIndexHandle.decodeFrom(data);
  _indexBlockHandle.decodeFrom(data);
}

void Footer::encodeTo(std::string* dst, const std::string& metaIndexHandle,
                      const std::string& indexBlockHandle)
{
  if (metaIndexHandle.empty() || indexBlockHandle.empty()) {
    printError("Footer: None handle");
  }

  if (metaIndexHandle.size() + indexBlockHandle.size() > MaxFooterSize - 8) {
    printError("handle to bigger to fill");
  }

  if (dst == nullptr) {
    printError("Footer: None dst");
  }

  size_t initSize = dst->size();
  dst->append(metaIndexHandle);
  dst->append(indexBlockHandle);
  // Append '\0'
  dst->resize(MaxFooterSize - 8, '\0');
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(TableMagicNumber >> 32));

  if (dst->size() - initSize != MaxFooterSize) {
    printError("Footer: size error");
  }
}

inline PosAndSize Footer::getMetaIndexPosAndSize() const
{ return {_metaIndexHandle.getPosition(), _metaIndexHandle.getSize()}; }

inline PosAndSize Footer::getIndexBlockPosAndSize() const
{ return {_indexBlockHandle.getPosition(), _indexBlockHandle.getSize()}; }

}

