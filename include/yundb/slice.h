#ifndef SLICE_H
#define SLICE_H

#include "util/error_print.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>


namespace yundb
{

class Slice
{
 public:
  Slice(const Slice& other) = default;
  Slice& operator=(const Slice& other) = default;
  bool operator==(const Slice& other) const
  {
     if (_size != other._size) return false; 
     return !(memcmp(_str, other._str, _size));
  }

  bool operator!=(const Slice& other) const
  {return !operator==(other);}

  Slice(Slice&& other) = default;
  Slice() : _str(nullptr), _size(0) {}
  Slice(int zero): _str(nullptr), _size(0) {} // For newNode(0, MaxHeight)
  Slice(const char* str) : _str(str), _size(strlen(str)) {}
  Slice(const char* str, const size_t size) : _str(str), _size(size) {}
  Slice(const std::string& str) : _str(str.data()), _size(str.size()) {}

  size_t size() const {return _size;}
  const char* data() const {return _str;}
  const char* begin() const {return data();}
  const char* end() const {return data() + size();}
  bool empty() const {return (_size == 0);}

  std::string toString() const {return std::string(_str, _size);}

  bool start_with(const Slice& startStr) const
  {
     if (_size < startStr.size()) return false;
     int rs = memcmp(_str, startStr._str, startStr.size());

     if (rs == 0) return true;
     return false;
  }

  bool end_with(const Slice& endStr) const
  {
     if (_size < endStr.size()) return false;

     int rs = memcmp(_str + (_size - endStr.size()), endStr._str, endStr.size());
     if (rs == 0) return true;
     return false;
  }

  void removePrefix(size_t len)
  {
     CERR_PRINT_WITH_CONDITIONAL(
      "Slice: removePrefix len more than size()",
      len <= size()
   );
     _str += len;
     _size -= len;
  }

  char operator[](size_t index) const
  {
     assert(index < size());
     return _str[index];
  }

  int cmp(const Slice& other) const;

 private:
  const char* _str;
  size_t _size;
};

inline int Slice::cmp(const Slice& other) const
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