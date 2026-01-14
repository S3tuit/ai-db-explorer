#pragma once
#include <stdio.h>

#ifdef ADBX_TESTLOG
  #define TLOG(fmt, ...) \
    fprintf(stderr, "[TEST] %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
  #define TLOG(fmt, ...) ((void)0)
#endif
