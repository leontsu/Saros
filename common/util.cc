#include "atomic_wrapper.hh"
#include "util.hh"

size_t decideParallelBuildNumber(size_t tuple_num) {
  if (tuple_num < 1000) return 1;

  for (size_t i = std::thread::hardware_concurrency(); i > 0; --i) {
    if (tuple_num % i == 0) {
      return i;
    }
    if (i == 1) ERR;
  }

  return 0;
}

void displayRusageRUMaxrss() {
  struct rusage r{};
  if (getrusage(RUSAGE_SELF, &r) != 0) ERR;
  printf("maxrss:\t%ld kB\n", r.ru_maxrss);
}

bool isReady(const std::vector<char> &readys) {
  for (const char &b : readys) {
    if (!loadAcquire(b)) return false;
  }
  return true;
}

void waitForReady(const std::vector<char> &readys) {
  while (!isReady(readys)) {
    cpu_pause();
  }
}

void sleepMs(size_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

