#ifndef ERROR_PRINT_H
#define ERROR_PRINT_H

#include <iostream>

namespace yundb
{
  #define CERR_PRINT_WITH_CONDITIONAL(PRINT_MESSAGE, CONDITIONAL_STATEMENTS) \
    if (CONDITIONAL_STATEMENTS) \
    std::cerr << PRINT_MESSAGE << "\n";
  
  #define CERR_PRINT(PRINT_MESSAGE) \
    std::cerr << PRINT_MESSAGE << "\n";
}

#endif