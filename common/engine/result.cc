#include "common/engine/result.hh"
#include "include/common.hh"

#include "common/cache_line_size.hh"
#include "common/result.hh"

using namespace std;

alignas(CACHE_LINE_SIZE) std::vector<Result> OzeResult;

void initResult() { OzeResult.resize(TotalThreadNum); }
