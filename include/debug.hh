#pragma once

#include <iostream>
#include <mutex>

#ifdef GLOBAL_VALUE_DEFINE
std::mutex cout_mutex;
void dump(int thid, std::string s) {
  std::lock_guard<std::mutex> lock(cout_mutex);
  std::cout << "[th#" << thid << "]: " << s << std::endl;
}
#else
extern std::mutex cout_mutex;
extern void dump(int thid, std::string s);
#endif

#define NNN                                                                    \
  do {                                                                         \
    fprintf(stderr, "%ld %16s %4d %16s\n", (long int)pthread_self(), __FILE__, \
            __LINE__, __func__);                                               \
    fflush(stderr);                                                            \
  } while (0)
#define ERR          \
  do {               \
    perror("ERROR"); \
    NNN;             \
    exit(1);         \
  } while (0)
