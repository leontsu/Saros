#pragma once
// --eval_sample_path: per-type commits, aborts and latency of the BoMB
// driver, sampled once per second (see the Sampler class).

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include "gflags/gflags.h"

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_string(eval_sample_path, "",
              "MEASUREMENT ONLY: 1 Hz per-type commit/abort/epoch series file "
              "(plus <path>.lat latency histograms). Empty (default) = off.");
#else
DECLARE_string(eval_sample_path);
#endif

namespace eval {

inline constexpr int kTypes = 8;
inline constexpr int kBucketsPerDecade = 40;
inline constexpr int kBuckets = 10 * kBucketsPerDecade;  // 1 us .. 1e10 us

struct alignas(64) PerThread {
    std::atomic<uint64_t> commits[kTypes];
    std::atomic<uint64_t> abort_exec[kTypes];
    std::atomic<uint64_t> abort_valid[kTypes];
    std::atomic<uint64_t> lat_sum_us[kTypes];
    std::atomic<uint64_t> hist_e2e[kTypes][kBuckets];
    std::atomic<uint64_t> hist_svc[kTypes][kBuckets];
};

inline bool g_on = false;
inline PerThread* g_threads = nullptr;
inline size_t g_nthreads = 0;
inline std::atomic<uint64_t> g_dispatched{0};

inline void bump(std::atomic<uint64_t>& a, uint64_t d = 1) {
    a.store(a.load(std::memory_order_relaxed) + d, std::memory_order_relaxed);
}

inline int bucket_of(uint64_t us) {
    if (us <= 1) return 0;
    int b = static_cast<int>(std::floor(kBucketsPerDecade * std::log10((double)us)));
    if (b < 0) b = 0;
    if (b >= kBuckets) b = kBuckets - 1;
    return b;
}

inline void on_commit(size_t thid, uint32_t type, uint64_t e2e_us,
                      uint64_t svc_us) {
    if (!g_on || thid >= g_nthreads || type >= (uint32_t)kTypes) return;
    PerThread& t = g_threads[thid];
    bump(t.commits[type]);
    bump(t.lat_sum_us[type], e2e_us);
    bump(t.hist_e2e[type][bucket_of(e2e_us)]);
    bump(t.hist_svc[type][bucket_of(svc_us)]);
}

inline void on_abort(size_t thid, uint32_t type, bool in_validation) {
    if (!g_on || thid >= g_nthreads || type >= (uint32_t)kTypes) return;
    PerThread& t = g_threads[thid];
    bump(in_validation ? t.abort_valid[type] : t.abort_exec[type]);
}

// Called only by the single dispatcher thread.
inline void on_dispatch(uint64_t n) {
    if (!g_on) return;
    bump(g_dispatched, n);
}

inline double mono_s() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}
inline double wall_s() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

// Writes one line per second: cumulative commits per transaction type, aborts
// during execution ("exec") and in commit() ("valid"), the summed end-to-end
// latency of committed requests, the global and the minimum worker epoch, and
// the number of short requests dispatched; plus events (load_begin,
// processing_start, ...). Every line is flushed, so a killed run keeps its
// series. <path>.lat holds two latency histograms per type (40 buckets per
// decade of microseconds), rewritten every 10 samples and at the end:
// e2e_us, from the request's start (enqueue for short requests, generation for
// L1) to the commit, including queueing and retries; svc_us, from when the
// worker obtained the request. Counters are per worker thread (single writer).
class Sampler {
public:
    // `types`: id -> name, captured once so columns stay aligned.
    void open(const std::string& path, size_t nthreads,
              const std::map<uint32_t, std::string>& types) {
        if (path.empty()) return;
        path_ = path;
        fh_ = std::fopen(path.c_str(), "w");
        if (fh_ == nullptr) return;
        g_nthreads = nthreads;
        g_threads = new PerThread[nthreads];
        std::memset(static_cast<void*>(g_threads), 0,
                    sizeof(PerThread) * nthreads);
        for (const auto& [id, name] : types) {
            if (id < (uint32_t)kTypes) { ids_.push_back(id); names_.push_back(name); }
        }
        std::fprintf(fh_, "#eval_sampler v1 buckets_per_decade=%d threads=%zu\n",
                     kBucketsPerDecade, nthreads);
        std::fprintf(fh_, "kind\tsecond\tmono_s\twall_s\tglobal_epoch\tmin_thread_epoch\tdispatched");
        for (const auto& n : names_) {
            std::fprintf(fh_, "\tcommits_%s\tabort_exec_%s\tabort_valid_%s\tlat_sum_us_%s",
                         n.c_str(), n.c_str(), n.c_str(), n.c_str());
        }
        std::fprintf(fh_, "\n");
        std::fflush(fh_);
        g_on = true;
        event("load_begin");
    }

    void event(const char* name) {
        if (fh_ == nullptr) return;
        std::fprintf(fh_, "event\t%s\t%.6f\t%.6f\n", name, mono_s(), wall_s());
        std::fflush(fh_);
    }

    void sample(uint64_t second, uint64_t global_epoch, uint64_t min_epoch) {
        if (fh_ == nullptr) return;
        std::fprintf(fh_, "sample\t%llu\t%.6f\t%.6f\t%llu\t%llu\t%llu",
                     (unsigned long long)second, mono_s(), wall_s(),
                     (unsigned long long)global_epoch,
                     (unsigned long long)min_epoch,
                     (unsigned long long)g_dispatched.load(std::memory_order_relaxed));
        for (uint32_t id : ids_) {
            uint64_t c = 0, ae = 0, av = 0, ls = 0;
            for (size_t i = 0; i < g_nthreads; ++i) {
                c += g_threads[i].commits[id].load(std::memory_order_relaxed);
                ae += g_threads[i].abort_exec[id].load(std::memory_order_relaxed);
                av += g_threads[i].abort_valid[id].load(std::memory_order_relaxed);
                ls += g_threads[i].lat_sum_us[id].load(std::memory_order_relaxed);
            }
            std::fprintf(fh_, "\t%llu\t%llu\t%llu\t%llu", (unsigned long long)c,
                         (unsigned long long)ae, (unsigned long long)av,
                         (unsigned long long)ls);
        }
        std::fprintf(fh_, "\n");
        std::fflush(fh_);
        if (++since_hist_ >= 10) { write_hist(); since_hist_ = 0; }
    }

    void finish() {
        if (fh_ == nullptr) return;
        write_hist();
        event("closed");
        std::fclose(fh_);
        fh_ = nullptr;
    }

private:
    void write_hist() {
        const std::string tmp = path_ + ".lat.tmp";
        FILE* h = std::fopen(tmp.c_str(), "w");
        if (h == nullptr) return;
        std::fprintf(h, "#eval_latency v1 buckets_per_decade=%d lower_us=10^(b/%d)"
                        " wall_s=%.6f\n", kBucketsPerDecade, kBucketsPerDecade, wall_s());
        std::fprintf(h, "type\tmetric\tbucket\tcount\n");
        for (size_t k = 0; k < ids_.size(); ++k) {
            const uint32_t id = ids_[k];
            for (int m = 0; m < 2; ++m) {
                for (int b = 0; b < kBuckets; ++b) {
                    uint64_t c = 0;
                    for (size_t i = 0; i < g_nthreads; ++i) {
                        c += (m == 0 ? g_threads[i].hist_e2e[id][b]
                                     : g_threads[i].hist_svc[id][b])
                                 .load(std::memory_order_relaxed);
                    }
                    if (c != 0) {
                        std::fprintf(h, "%s\t%s\t%d\t%llu\n", names_[k].c_str(),
                                     m == 0 ? "e2e_us" : "svc_us", b,
                                     (unsigned long long)c);
                    }
                }
            }
        }
        std::fclose(h);
        std::rename(tmp.c_str(), (path_ + ".lat").c_str());
    }

    std::string path_;
    FILE* fh_ = nullptr;
    std::vector<uint32_t> ids_;
    std::vector<std::string> names_;
    int since_hist_ = 0;
};

}  // namespace eval
