// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef SNAPPY_WRAPPER_H
#define SNAPPY_WRAPPER_H

#include <snappy.h>

namespace yundb
{

inline bool Snappy_Compress(const char* input, size_t length,
                            std::string* output)
{
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output)
{return snappy::RawUncompress(input, length, output);}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result)
{return snappy::GetUncompressedLength(input, length, result);}

}



#endif