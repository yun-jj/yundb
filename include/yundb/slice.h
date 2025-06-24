#ifndef SLICE_H
#define SLICE_H

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>
#include <iostream>

namespace yundb
{

class Slice
{
 public:
  Slice(const Slice& other) = default;
  Slice& operator=(const Slice& other) = default;
  Slice(Slice&& other) = default;
  Slice() : _str(nullptr), _size(0) {}
  Slice(const char* str) : _str(str), _size(strlen(str)) {}
  Slice(const char* str, const size_t size) : _str(str), _size(size) {}
  Slice(const std::string& str) : _str(str.data()), _size(str.size()) {}

  size_t size() const {return _size;}
  const char* data() const {return _str;}
  const char* begin() const {return data();}
  const char* end() const {return data() + size();}
  bool empty() const {return (_size == 0);}
  void removePrefix(size_t len)
  {
      if (len <= size())
         std::cerr << "Slice: removePrefix len more than size()\n"; 
      _str += len;
      _size -= len;
  }
  char operator[](size_t index) const
  {
      assert(index < size());
      return _str[index];
  }

  bool cmp(const Slice& other) const;

 private:
  const char* _str;
  size_t _size;
};

inline bool Slice::cmp(const Slice& other) const
{
   size_t min = _size < other._size ? _size : other._size;
   int rs = memcmp(_str, other._str, min);

   if (rs == 0)
   {
      if (_size < other._size)
         rs = -1;
      else if (_size > other._size)
         rs = +1;
   }
   return rs;
}

}

#endif