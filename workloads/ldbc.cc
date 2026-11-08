// TuskFlow's LDBC SNB Q2 workload: open-loop short transactions on Persons and
// Messages, and --q2_clients threads that run Q2 (q2_worker).
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "common/atomic_wrapper.hh"
#include "common/backoff.hh"
#include "common/debug.hh"
#include "common/heap_object.hh"
#include "common/random.hh"
#include "common/result.hh"
#include "common/tuple_body.hh"
#include "common/util.hh"
#include "common/workload.hh"

DEFINE_string(ldbc_path, "", "LDBC SNB raw CSV directory (.../graphs/csv/raw/composite-merged-fk).");
DEFINE_uint64(q2_clients, 1, "Threads that run Q2 back to back (taken from --thread_num).");
DEFINE_uint64(q2_stmt_delay_us, 0, "Client round trip after each Q2 statement (the search, then each "
              "candidate's update), with the transaction open.");
DEFINE_bool(q2_all_countries, false, "Every Person 2..4 knows hops from the hub is a candidate, as in "
            "TuskFlow's original Q2 (no Country).");
DEFINE_bool(q2_single, false, "Run one Q2, started --tf_lr_delay_s seconds in and retried until it commits.");
DEFINE_int64(q2_hub, -1, "The source Person of every Q2 is hub number q2_hub (-1 = a random hub per Q2).");
DEFINE_uint64(tf_rate, 10000, "Short transactions per second (all short threads together).");
DEFINE_uint64(tf_lr_delay_s, 60, "--q2_single: seconds after start before the Q2 starts.");
DEFINE_uint64(ldbc_read_ratio, 80, "Percentage of short transactions that are read-only.");

using namespace std;

enum class Storage : std::uint32_t {
  Graph = 0,
  Size,
};

constexpr size_t kValSize = 4;

struct GraphRecord {
  alignas(CACHE_LINE_SIZE)
  std::uint64_t id_;
  char val_[kValSize];

  static void CreateKey(uint64_t id, char *out) {
    assign_as_bigendian(id, &out[0]);
  }
};

namespace {
// TuskFlow's Q2 parameters: each client's Country (about 190-200 Persons each
// in SF3), a candidate's knows distance, a hub's minimum degree (x average).
const char* const kQ2Countries =
    "Ethiopia,South_Korea,Niger,Colombia,Greece,Senegal,Romania,Iran,Australia,Tunisia,Cuba,"
    "Argentina,Sri_Lanka,South_Africa,Egypt,Kenya";
constexpr uint64_t kQ2MinPath = 2;
constexpr uint64_t kQ2MaxPath = 4;
constexpr uint64_t kQ2HubFactor = 10;
// TuskFlow's --ops of the short transactions.
constexpr uint64_t kLdbcOps = 10;

uint64_t g_persons = 0, g_messages = 0;
std::vector<uint32_t> g_msg_off, g_msg;          // CSR: Person -> its Messages (record keys)
std::vector<uint32_t> g_other_in;                // Person -> number of other incoming relationships
std::vector<uint32_t> g_knows_off, g_knows;      // CSR: Person -> friends (Person keys), both directions
std::vector<int16_t> g_person_country;
std::vector<uint32_t> g_mtag_off, g_mtag;        // CSR: Message index -> Tag ids
uint32_t g_tag_limit = 0;
std::vector<uint32_t> g_hubs;
std::vector<int16_t> g_q2_client_country;

void split(const std::string& line, std::vector<std::string>& cols) {
  cols.clear();
  size_t s = 0;
  while (true) {
    size_t b = line.find('|', s);
    if (b == std::string::npos) { cols.emplace_back(line.substr(s)); break; }
    cols.emplace_back(line.substr(s, b - s));
    s = b + 1;
  }
}
template <typename F>
void for_rows(const std::string& folder, F&& f) {
  std::ifstream in(FLAGS_ldbc_path + folder + "/part-0.csv");
  if (!in) { fprintf(stderr, "cannot open %s\n", (FLAGS_ldbc_path + folder + "/part-0.csv").c_str()); exit(1); }
  std::string line; std::vector<std::string> cols;
  std::getline(in, line);   // header
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    split(line, cols);
    f(cols);
  }
}
// Loads an LDBC SNB raw-CSV data set (graphs/csv/raw/composite-merged-fk).
// Persons become records 0..P-1 and Messages records P..P+M-1. No transaction
// changes the graph structure, so it is kept as static in-memory adjacency.
void load_graph() {
  const double t0 = (double)clock() / CLOCKS_PER_SEC;
  std::unordered_map<int64_t, uint32_t> person;   // LDBC id -> Person key
  for_rows("/dynamic/Person", [&](auto& c) { person.emplace(std::stoll(c[3]), (uint32_t)person.size()); });
  g_persons = person.size();
  std::vector<uint32_t> creator;                  // Message index -> Person key
  std::unordered_map<int64_t, uint32_t> post_idx, comment_idx;   // LDBC id -> Message index
  auto msgs = [&](const char* folder, int creator_col, std::unordered_map<int64_t, uint32_t>& idx) {
    for_rows(folder, [&](auto& c) {
      idx.emplace(std::stoll(c[3]), (uint32_t)creator.size());
      auto it = person.find(std::stoll(c[creator_col]));
      creator.push_back(it == person.end() ? UINT32_MAX : it->second);
    });
  };
  msgs("/dynamic/Post", 10, post_idx);
  msgs("/dynamic/Comment", 8, comment_idx);
  g_messages = creator.size();
  g_msg_off.assign(g_persons + 1, 0);
  for (uint32_t p : creator) if (p != UINT32_MAX) ++g_msg_off[p + 1];
  for (size_t i = 0; i < g_persons; ++i) g_msg_off[i + 1] += g_msg_off[i];
  g_msg.assign(g_msg_off[g_persons], 0);
  {
    std::vector<uint32_t> pos(g_msg_off.begin(), g_msg_off.end() - 1);
    for (size_t m = 0; m < creator.size(); ++m)
      if (creator[m] != UINT32_MAX) g_msg[pos[creator[m]]++] = (uint32_t)(g_persons + m);
  }
  g_other_in.assign(g_persons, 0);
  for_rows("/dynamic/Forum_hasMember_Person", [&](auto& c) {
    auto it = person.find(std::stoll(c[4])); if (it != person.end()) ++g_other_in[it->second]; });
  std::vector<std::pair<uint32_t, uint32_t>> knows;
  for_rows("/dynamic/Person_knows_Person", [&](auto& c) {
    auto a = person.find(std::stoll(c[3])), b = person.find(std::stoll(c[4]));
    if (a == person.end() || b == person.end()) return;
    ++g_other_in[b->second];                      // incoming knows
    knows.emplace_back(a->second, b->second);
  });
  g_knows_off.assign(g_persons + 1, 0);
  for (auto& e : knows) { ++g_knows_off[e.first + 1]; ++g_knows_off[e.second + 1]; }
  for (size_t i = 0; i < g_persons; ++i) g_knows_off[i + 1] += g_knows_off[i];
  g_knows.assign(g_knows_off[g_persons], 0);
  {
    std::vector<uint32_t> pos(g_knows_off.begin(), g_knows_off.end() - 1);
    for (auto& e : knows) { g_knows[pos[e.first]++] = e.second; g_knows[pos[e.second]++] = e.first; }
  }
  std::unordered_map<std::string, std::pair<std::string, std::string>> place;   // id -> (name, type)
  std::unordered_map<std::string, std::string> part_of;
  for_rows("/static/Place", [&](auto& c) { place[c[0]] = {c[1], c[3]}; part_of[c[0]] = c[4]; });
  std::unordered_map<std::string, int16_t> country_id;
  std::vector<std::string> country_name;
  g_person_country.assign(g_persons, -1);
  uint32_t i = 0;
  for_rows("/dynamic/Person", [&](auto& c) {   // same row order as the Person keys above
    std::string cid = c[10];
    if (place[cid].second == "City") cid = part_of[cid];
    auto it = country_id.emplace(place[cid].first, (int16_t)country_name.size());
    if (it.second) country_name.push_back(place[cid].first);
    g_person_country[i++] = it.first->second;
  });
  std::vector<std::pair<uint32_t, uint32_t>> mt;   // (Message index, Tag id)
  auto tags = [&](const char* folder, std::unordered_map<int64_t, uint32_t>& idx) {
    for_rows(folder, [&](auto& c) {
      auto it = idx.find(std::stoll(c[2]));
      if (it == idx.end()) return;
      const uint32_t t = (uint32_t)std::stoul(c[3]);
      mt.emplace_back(it->second, t);
      g_tag_limit = std::max(g_tag_limit, t + 1);
    });
  };
  tags("/dynamic/Post_hasTag_Tag", post_idx);
  tags("/dynamic/Comment_hasTag_Tag", comment_idx);
  g_mtag_off.assign(g_messages + 1, 0);
  for (auto& e : mt) ++g_mtag_off[e.first + 1];
  for (size_t m = 0; m < g_messages; ++m) g_mtag_off[m + 1] += g_mtag_off[m];
  g_mtag.assign(mt.size(), 0);
  {
    std::vector<uint32_t> pos(g_mtag_off.begin(), g_mtag_off.end() - 1);
    for (auto& e : mt) g_mtag[pos[e.first]++] = e.second;
  }
  std::vector<uint32_t> deg(g_persons);   // distinct friends
  double sum = 0;
  for (size_t p = 0; p < g_persons; ++p) {
    std::vector<uint32_t> f(g_knows.begin() + g_knows_off[p], g_knows.begin() + g_knows_off[p + 1]);
    std::sort(f.begin(), f.end());
    deg[p] = (uint32_t)(std::unique(f.begin(), f.end()) - f.begin());
    sum += deg[p];
  }
  for (size_t p = 0; p < g_persons; ++p)
    if (deg[p] >= kQ2HubFactor * sum / g_persons) g_hubs.push_back((uint32_t)p);
  const std::string list = kQ2Countries;
  for (size_t b = 0; b <= list.size();) {
    size_t e = list.find(',', b);
    if (e == std::string::npos) e = list.size();
    auto it = country_id.find(list.substr(b, e - b));
    if (it == country_id.end()) { fprintf(stderr, "q2: unknown country %s\n", list.substr(b, e - b).c_str()); exit(1); }
    g_q2_client_country.push_back(it->second);
    b = e + 1;
  }
  printf("#ldbc_q2_graph\thubs=%zu\tavg_friends=%.1f\tmessage_tags=%zu\tcountries=%zu\n", g_hubs.size(),
         sum / g_persons, mt.size(), country_name.size());
  printf("#ldbc_graph\tpersons=%lu\tmessages=%lu\tknows=%zu\tload_cpu_s=%.1f\n", g_persons, g_messages,
         knows.size(), (double)clock() / CLOCKS_PER_SEC - t0);
}

void load_records(size_t thid, uint64_t start, uint64_t end) {
  MasstreeWrapper<Tuple>::thread_init(thid);
  for (auto i = start; i <= end; ++i) {
    SimpleKey<8> key;
    GraphRecord::CreateKey(i, key.ptr());
    HeapObject obj;
    obj.allocate<GraphRecord>();
    GraphRecord& record = obj.ref();
    record.id_ = i;
    Tuple* tmp = new Tuple();
    tmp->init(thid, TupleBody(key.view(), std::move(obj)), nullptr);
    Masstrees[get_storage(Storage::Graph)].insert_value(key.view(), tmp);
  }
}

void make_db(uint64_t records) {
  size_t maxthread = decideParallelBuildNumber(records);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < maxthread; ++i)
    thv.emplace_back(load_records, i, i * (records / maxthread),
                     (i + 1) * (records / maxthread) - 1);
  for (auto &th : thv) th.join();
}

constexpr int kBuckets = 400;              // 40 per decade, 1 us .. 10^10 us
// Short transactions by phase: before, during and after the --q2_single Q2.
std::atomic<uint64_t> g_lat[3][kBuckets];
std::atomic<uint64_t> g_long_lat[kBuckets];
std::atomic<uint64_t> g_sec[4096];         // short commits per second
std::atomic<uint64_t> g_long_sec[4096];    // Q2 commits per second
std::atomic<int> g_phase{0};
std::atomic<uint64_t> g_lr_start_us{0}, g_lr_end_us{0}, g_lr_attempts{0}, g_lr_exec_us{0}, g_lr_cand{0};
std::atomic<uint64_t> g_long_commits{0}, g_long_aborts{0}, g_long_reads{0};
uint64_t g_t0_tsc = 0;
inline double now_us() { return (double)(rdtscp() - g_t0_tsc) / (double)FLAGS_clocks_per_us; }
inline int bucket(double us) {
  if (us < 1) return 0;
  int b = (int)std::floor(std::log10(us) * 40);
  return std::min(std::max(b, 0), kBuckets - 1);
}

struct Op { uint64_t key; bool write; bool read; };

// Runs a short transaction over ops, retrying it until it commits (true) or
// the run ends (false).
bool run_tx(TxExecutor& tx, const std::vector<Op>& ops) {
  std::vector<SimpleKey<8>> skeys(ops.size());
  for (size_t i = 0; i < ops.size(); ++i) GraphRecord::CreateKey(ops[i].key, skeys[i].ptr());
RETRY:
  if (tx.isLeader()) tx.leaderWork();
  if (loadAcquire(tx.quit_)) return false;
  tx.begin();
  for (size_t i = 0; i < ops.size(); ++i) {
    TupleBody* body = nullptr;
    uint64_t id = ops[i].key; char v0 = 0;
    if (ops[i].read) {
      tx.read(Storage::Graph, skeys[i].view(), &body);
      if (tx.status_ == TransactionStatus::aborted) goto ABORT;
      GraphRecord& t = body->get_value().cast_to<GraphRecord>();
      id = t.id_; v0 = t.val_[0];
    }
    if (ops[i].write) {
      HeapObject obj;
      obj.template allocate<GraphRecord>();
      GraphRecord& nt = obj.ref();
      nt.id_ = id;
      memset(nt.val_, 0, kValSize);
      nt.val_[0] = (char)(v0 ^ 1);
      tx.write(Storage::Graph, skeys[i].view(), TupleBody(skeys[i].view(), std::move(obj)));
      if (tx.status_ == TransactionStatus::aborted) goto ABORT;
    }
    if (loadAcquire(tx.quit_)) { tx.abort(); return false; }
  }
  if (!tx.commit()) {
    tx.abort();
    if (tx.status_ == TransactionStatus::invalid || loadAcquire(tx.quit_)) return false;
    ++tx.result_->local_abort_counts_;
    goto RETRY;
  }
  storeRelease(tx.result_->local_commit_counts_, loadAcquire(tx.result_->local_commit_counts_) + 1);
  return true;
ABORT:
  tx.abort();
  ++tx.result_->local_abort_counts_;
  goto RETRY;
}

// TuskFlow's RandomFriendMessages{Read,Update}Tx: from a uniformly chosen
// Person, read the Person, then pick kLdbcOps - 1 of its incoming relationships
// uniformly; for each pick that is a hasCreator edge, read the Message
// (read-only transaction) or read and update it (update transaction, with
// probability 100 - --ldbc_read_ratio percent). The Person's other incoming
// relationships (Forum hasMember, knows) only shape the pick distribution.
// Open loop at --tf_rate transactions per second over all short threads;
// latency runs from a transaction's scheduled arrival to its commit.
void short_worker(size_t thid, size_t nreg, char& ready, const bool& start, const bool& quit) {
  Backoff backoff(FLAGS_clocks_per_us);
  TxExecutor tx(thid, backoff, (Result*)&OzeResult[thid], quit);
  Xoroshiro128Plus rnd;
  rnd.init();
  for (size_t i = 0; i < thid * 7 + 1; ++i) rnd.next();
  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  const double interval_us = 1e6 * (double)nreg / (double)FLAGS_tf_rate;
  double next_us = interval_us * ((double)thid / (double)nreg);   // stagger
  std::vector<Op> ops;
  const uint64_t picks = kLdbcOps > 1 ? kLdbcOps - 1 : 0;
  while (!loadAcquire(quit)) {
    while (now_us() < next_us) {
      if (tx.isLeader()) tx.leaderWork();
      if (loadAcquire(quit)) return;
      _mm_pause();
    }
    const double arrival = next_us;
    next_us += interval_us;
    const bool update = (rnd.next() % 100) >= FLAGS_ldbc_read_ratio;
    const uint32_t p = (uint32_t)(rnd.next() % g_persons);
    ops.clear();
    ops.push_back({p, false, true});
    const uint64_t nm = g_msg_off[p + 1] - g_msg_off[p], nin = nm + g_other_in[p];
    for (uint64_t i = 0; i < picks && nin; ++i) {
      const uint64_t r = rnd.next() % nin;
      if (r >= nm) continue;                                // not a hasCreator edge
      const uint64_t m = g_msg[g_msg_off[p] + r];
      bool dup = false;
      for (auto& o : ops) if (o.key == m) { dup = true; break; }
      if (!dup) ops.push_back({m, update, true});
    }
    if (!run_tx(tx, ops)) return;
    const double done = now_us();
    g_lat[g_phase.load(std::memory_order_relaxed)][bucket(done - arrival)].fetch_add(1, std::memory_order_relaxed);
    const size_t s = (size_t)(done / 1e6);
    if (s < 4096) g_sec[s].fetch_add(1, std::memory_order_relaxed);
  }
}

// Keeps an idle thread's epoch current, so that it does not hold back reclamation.
void idle_until(size_t thid, double until_us, const bool& quit) {
  while (now_us() < until_us && !loadAcquire(quit)) {
    atomicStoreThLocalEpoch(thid, atomicLoadGE());
    sleepMs(1);
  }
}

// TuskFlow's Q2 (FindExpertsLongRunningTx, a variant of the LDBC BI query
// "Experts in social circle") with the BI query's Country parameter. From a hub
// Person, the candidates are the Persons first reached at knows distance
// kQ2MinPath..kQ2MaxPath who live in this client's Country; the source Tags are
// the Tags of the hub's Messages. For each candidate, Q2 counts the Tags of its
// Messages that are source Tags and writes the count to the candidate. The
// traversal reads only the static graph structure; the transaction reads and
// writes the candidates. Client i takes the i-th of kQ2Countries, so clients
// write disjoint Persons. --q2_stmt_delay_us = d: the search is one statement
// and each candidate's update another, each followed by a round trip of d.
void q2_worker(size_t thid, char& ready, const bool& start, const bool& quit) {
  Backoff backoff(FLAGS_clocks_per_us);
  TxExecutor tx(thid, backoff, (Result*)&OzeResult[thid], quit);
  Xoroshiro128Plus rnd;
  rnd.init();
  for (size_t i = 0; i < 977 + 131 * (TotalThreadNum - 1 - thid); ++i) rnd.next();
  const size_t client = thid - (TotalThreadNum - FLAGS_q2_clients);
  const int16_t country = g_q2_client_country[client % g_q2_client_country.size()];
  std::vector<uint32_t> seen(g_persons, 0), src(g_tag_limit, 0), front, next, cand;
  uint32_t stamp = 0;
  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  auto round_trip = [&]() {
    const double until = now_us() + (double)FLAGS_q2_stmt_delay_us;
    while (now_us() < until)
      if (loadAcquire(quit)) return false;
    return true;
  };
  if (FLAGS_q2_single) {
    idle_until(thid, 1e6 * (double)FLAGS_tf_lr_delay_s, quit);
    g_lr_start_us = (uint64_t)now_us();
    g_phase = 1;
  }
  while (!loadAcquire(quit)) {
    const uint32_t s = FLAGS_q2_hub >= 0 ? g_hubs[FLAGS_q2_hub % g_hubs.size()] : g_hubs[rnd.next() % g_hubs.size()];
    const double t0 = now_us();
    uint64_t aborts = 0;
    for (;;) {
      tx.begin();
      ++stamp;
      cand.clear(); front.assign(1, s); seen[s] = stamp;
      for (uint64_t dist = 1; dist <= kQ2MaxPath && !front.empty(); ++dist) {
        next.clear();
        for (uint32_t u : front)
          for (uint64_t j = g_knows_off[u]; j < g_knows_off[u + 1]; ++j) {
            const uint32_t v = g_knows[j];
            if (seen[v] == stamp) continue;
            seen[v] = stamp;
            next.push_back(v);
            if (dist >= kQ2MinPath && (FLAGS_q2_all_countries || g_person_country[v] == country))
              cand.push_back(v);
          }
        front.swap(next);
      }
      for (uint64_t j = g_msg_off[s]; j < g_msg_off[s + 1]; ++j) {
        const uint64_t m = g_msg[j] - g_persons;
        for (uint64_t t = g_mtag_off[m]; t < g_mtag_off[m + 1]; ++t) src[g_mtag[t]] = stamp;
      }
      if (FLAGS_q2_single) g_lr_cand = cand.size();
      if (FLAGS_q2_stmt_delay_us && !round_trip()) { tx.abort(); goto DONE; }
      for (uint32_t c : cand) {
        uint64_t count = 0;
        for (uint64_t j = g_msg_off[c]; j < g_msg_off[c + 1]; ++j) {
          const uint64_t m = g_msg[j] - g_persons;
          for (uint64_t t = g_mtag_off[m]; t < g_mtag_off[m + 1]; ++t) count += src[g_mtag[t]] == stamp;
        }
        SimpleKey<8> key;
        GraphRecord::CreateKey(c, key.ptr());
        TupleBody* body = nullptr;
        tx.read(Storage::Graph, key.view(), &body);
        if (tx.status_ == TransactionStatus::aborted) goto ABORT;
        {
          HeapObject obj;
          obj.template allocate<GraphRecord>();
          GraphRecord& nt = obj.ref();
          nt.id_ = body->get_value().cast_to<GraphRecord>().id_;
          memset(nt.val_, 0, kValSize);
          memcpy(nt.val_, &count, std::min<size_t>(sizeof(count), kValSize));
          tx.write(Storage::Graph, key.view(), TupleBody(key.view(), std::move(obj)));
        }
        if (tx.status_ == TransactionStatus::aborted) goto ABORT;
        if (FLAGS_q2_stmt_delay_us && !round_trip()) { tx.abort(); goto DONE; }
        if (loadAcquire(quit)) { tx.abort(); goto DONE; }
      }
      if (FLAGS_q2_single) g_lr_exec_us = (uint64_t)now_us();
      if (tx.commit()) break;
    ABORT:
      tx.abort();
      if (tx.status_ == TransactionStatus::invalid || loadAcquire(quit)) goto DONE;
      ++tx.result_->local_abort_counts_;
      ++aborts;
      if (FLAGS_q2_single) g_lr_attempts = aborts + 1;
    }
    storeRelease(tx.result_->local_commit_counts_, loadAcquire(tx.result_->local_commit_counts_) + 1);
    {
      const double done = now_us();
      g_long_commits.fetch_add(1); g_long_aborts.fetch_add(aborts); g_long_reads.fetch_add(cand.size());
      g_long_lat[bucket(done - t0)].fetch_add(1, std::memory_order_relaxed);
      const size_t sec = (size_t)(done / 1e6);
      if (sec < 4096) g_long_sec[sec].fetch_add(1, std::memory_order_relaxed);
    }
    if (FLAGS_q2_single) { g_lr_end_us = (uint64_t)now_us(); g_lr_attempts = aborts + 1; g_phase = 2; break; }
  }
DONE:
  idle_until(thid, 1e18, quit);
}

double pct(const std::atomic<uint64_t>* h, double q) {
  uint64_t tot = 0;
  for (int b = 0; b < kBuckets; ++b) tot += h[b].load();
  if (!tot) return NAN;
  uint64_t acc = 0;
  for (int b = 0; b < kBuckets; ++b) {
    acc += h[b].load();
    if (acc >= q * tot) return std::pow(10.0, (b + 0.5) / 40.0) / 1000.0;   // ms
  }
  return NAN;
}
}  // namespace

int main(int argc, char* argv[]) try {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  gflags::SetUsageMessage("LDBC SNB Q2 workload for Saros and Oze.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  load_graph();
  init((uint32_t)Storage::Size);
  printf("#FLAGS_ldbc_read_ratio:\t%lu\n#FLAGS_q2_clients:\t%lu\n#FLAGS_q2_stmt_delay_us:\t%lu\n",
         FLAGS_ldbc_read_ratio, FLAGS_q2_clients, FLAGS_q2_stmt_delay_us);
  printf("#FLAGS_q2_all_countries:\t%d\n#FLAGS_q2_single:\t%d\n#FLAGS_q2_hub:\t%ld\n", (int)FLAGS_q2_all_countries,
         (int)FLAGS_q2_single, (long)FLAGS_q2_hub);
  printf("#FLAGS_tf_rate:\t%lu\n#FLAGS_tf_lr_delay_s:\t%lu\n", FLAGS_tf_rate, FLAGS_tf_lr_delay_s);
  make_db(g_persons + g_messages);
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  const size_t nlong = FLAGS_q2_clients;
  if (nlong < 1 || nlong >= TotalThreadNum) { fprintf(stderr, "--q2_clients must be in [1, thread_num)\n"); return 1; }
  const size_t T = TotalThreadNum, nreg = T - nlong;
  std::vector<char> readys(T);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < nreg; ++i)
    thv.emplace_back(short_worker, i, nreg, std::ref(readys[i]), std::ref(start), std::ref(quit));
  for (size_t i = nreg; i < T; ++i)
    thv.emplace_back(q2_worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit));
  waitForReady(readys);
  g_t0_tsc = rdtscp();
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
    // floor: the lowest reclamation floor announced by a live transaction (-1 if none).
    const uint64_t floor = ReclamationFloorAnnouncer::min_live();
    printf("#ldbc_sec\t%zu\tshort=%lu\tlong=%lu\tphase=%d\tge=%lu\tfloor=%ld\n", i,
           (unsigned long)g_sec[i].load(), (unsigned long)g_long_sec[i].load(), g_phase.load(),
           (unsigned long)atomicLoadGE(),
           floor == std::numeric_limits<uint64_t>::max() ? -1L : (long)floor);
    fflush(stdout);
  }
  storeRelease(quit, true);
  for (auto& th : thv) th.join();
  const char* names[3] = {"before", "during", "after"};
  for (int ph = 0; ph < 3; ++ph) {
    uint64_t n = 0;
    for (int b = 0; b < kBuckets; ++b) n += g_lat[ph][b].load();
    printf("#ldbc_short_latency\t%s\tcommits=%lu\tp50_ms=%.3f\tp99_ms=%.3f\tp999_ms=%.3f\tmax_bucket_ms=%.1f\n",
           names[ph], (unsigned long)n, pct(g_lat[ph], 0.5), pct(g_lat[ph], 0.99), pct(g_lat[ph], 0.999), pct(g_lat[ph], 1.0));
  }
  // Latency histograms, as (bucket midpoint in ms, count).
  for (int ph = 0; ph < 3; ++ph)
    for (int b = 0; b < kBuckets; ++b)
      if (uint64_t c = g_lat[ph][b].load())
        printf("#ldbc_short_hist\t%s\t%.6g\t%lu\n", names[ph], std::pow(10.0, (b + 0.5) / 40.0) / 1000.0, (unsigned long)c);
  for (int b = 0; b < kBuckets; ++b)
    if (uint64_t c = g_long_lat[b].load())
      printf("#ldbc_long_hist\t%.6g\t%lu\n", std::pow(10.0, (b + 0.5) / 40.0) / 1000.0, (unsigned long)c);
  printf("#ldbc_q2\tcommits=%lu\tper_min=%.1f\taborted_attempts=%lu\tavg_writes=%.0f\tp50_ms=%.3f\tp99_ms=%.3f\n",
         (unsigned long)g_long_commits.load(), 60.0 * g_long_commits.load() / (double)FLAGS_extime,
         (unsigned long)g_long_aborts.load(),
         g_long_commits.load() ? (double)g_long_reads.load() / g_long_commits.load() : 0.0,
         pct(g_long_lat, 0.5), pct(g_long_lat, 0.99));
  if (FLAGS_q2_single)
    printf("#ldbc_q2_single\tstart_s=%.3f\texec_done_s=%.3f\tend_s=%.3f\tduration_s=%.3f\tattempts=%lu\tcommitted=%d"
           "\tcandidates=%lu\n", g_lr_start_us / 1e6, g_lr_exec_us / 1e6, g_lr_end_us / 1e6,
           g_lr_end_us ? (g_lr_end_us - g_lr_start_us) / 1e6 : NAN, (unsigned long)g_lr_attempts.load(),
           g_lr_end_us != 0, (unsigned long)g_lr_cand.load());
  if (FLAGS_free_retired_history_chunks) {
    auto& c = ChunkRetirementRegistry::counters();
    printf("#ldbc_chunk_free\tchunks_unlinked=%lu\tchunks_freed=%lu\treclaim_passes=%lu\tpending_bytes=%lu"
           "\tpeak_pending_bytes=%lu\n", (unsigned long)c.chunks_unlinked.load(), (unsigned long)c.chunks_freed.load(),
           (unsigned long)c.reclaim_passes.load(), (unsigned long)c.pending_bytes.load(),
           (unsigned long)c.peak_pending_bytes.load());
  }
  for (unsigned int i = 0; i < T; ++i) OzeResult[0].addLocalAllResult(OzeResult[i]);
  OzeResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, T);
  return 0;
} catch (bad_alloc) {
  ERR;
}
