// The obsolete-row version of BoMB (--bomb_mark_obsolete), on either engine, on a small database.
#include <map>
#include <set>
#include <string>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#include "include/atomic_tool.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "common/backoff.hh"
#include "common/util.hh"
#include "workloads/bomb.hh"

#include "gtest/gtest.h"

namespace {

using Workload = BombWorkload<Tuple, void>;

constexpr uint32_t kFactory = 1;
constexpr uint32_t kProducts = 4;

class BombObsoleteTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        FLAGS_thread_num = 2;
        FLAGS_bomb_factory_size = 1;
        FLAGS_bomb_product_size = 40;
        FLAGS_bomb_work_size = 128;
        FLAGS_bomb_material_size = 200;
        FLAGS_bomb_tree_num_per_product = 2;
        FLAGS_bomb_base_tree_size = 3;
        FLAGS_bomb_material_per_wip = 2;
        FLAGS_bomb_base_product_size_per_factory = kProducts;
        FLAGS_bomb_use_cache = false;
        FLAGS_insert_only_single_writer = true;
#ifdef ENGINE_SAROS
        FLAGS_saros = true;
        FLAGS_validate_insert_only_reads = true;
#endif
        init(Workload::getTableNum());
        initResult();
        ItemIdCounter.store(0);
        Workload::makeDB(nullptr);
    }

    static void TearDownTestSuite() {
        FLAGS_bomb_mark_obsolete = false;
    }

    void SetUp() override {
        MasstreeWrapper<Tuple>::thread_init(0);
        backoff_.reset(new Backoff(FLAGS_clocks_per_us));
        tx_.reset(new TxExecutor(0, *backoff_, &OzeResult[0], quit_));
    }

    Workload::Query factory_query() {
        Workload::Query q;
        q.args.f_id = kFactory;
        return q;
    }

    void run_l1() {
        auto q = factory_query();
        tx_->begin();
        workload_.run_update_product_cost_master(*tx_, q);
        ASSERT_NE(tx_->status_, TransactionStatus::aborted);
        ASSERT_TRUE(tx_->commit());
    }

    // S3's delete of one product; returns the product removed.
    uint32_t remove_one_product() {
        auto q = factory_query();
        const std::set<uint32_t> before = manufactured();
        tx_->begin();
        Status st = workload_.run_delete_product(*tx_, q);
        EXPECT_EQ(st, Status::OK);
        EXPECT_TRUE(tx_->commit());
        const std::set<uint32_t> after = manufactured();
        for (uint32_t p : before) {
            if (after.count(p) == 0) return p;
        }
        ADD_FAILURE() << "no product was removed";
        return 0;
    }

    std::set<uint32_t> manufactured() {
        std::vector<uint32_t> ids;
        tx_->reconnoiter_begin();
        workload_.select_im_by_factory(*tx_, kFactory, ids);
        tx_->reconnoiter_end();
        tx_->abort();
        return std::set<uint32_t>(ids.begin(), ids.end());
    }

    struct Row { bool obsolete; TxID writer; };
    std::map<uint32_t, Row> cost_rows() {
        std::map<uint32_t, Row> rows;
        SimpleKey<8> low, up;
        ProductCostMaster::CreateKey(kFactory, 0, low.ptr());
        ProductCostMaster::CreateKey(kFactory + 1, 0, up.ptr());
        std::vector<TupleBody*> result;
        tx_->begin();
        tx_->scan(Storage::ProductCostMaster, low.view(), false, up.view(), true, result);
        for (const auto& re : tx_->read_set_) {
            const auto& pc = re.ver_->body_.get_value().template cast_to<ProductCostMaster>();
            rows[pc.pc_product_id] = Row{pc.pc_obsolete, re.txid_};
        }
        tx_->abort();
        return rows;
    }

    // S2 up to its commit; returns the vouchers it would insert.
    size_t run_s2() {
        auto q = factory_query();
        tx_->begin();
        workload_.run_issue_journal_voucher(*tx_, q);
        return tx_->write_set_.size();
    }

    Workload workload_;
    std::unique_ptr<Backoff> backoff_;
    std::unique_ptr<TxExecutor> tx_;
    bool quit_ = false;
};

// One test, because rows stay deleted: first without the marking, then with it.
TEST_F(BombObsoleteTest, MarkingKeepsOneWriterAndS2SkipsTheRow) {
    FLAGS_bomb_mark_obsolete = false;
    run_l1();
    const uint32_t removed = remove_one_product();
    run_l1();
    auto rows = cost_rows();
    ASSERT_EQ(rows.size(), kProducts);
    EXPECT_FALSE(rows[removed].obsolete);
    std::set<uint64_t> writers;
    for (const auto& [p, row] : rows) writers.insert(row.writer.obj_);
    EXPECT_EQ(writers.size(), 2u);  // the removed product's row keeps its older writer
    EXPECT_EQ(run_s2(), static_cast<size_t>(kProducts));
    tx_->status_ = TransactionStatus::aborted;
    tx_->abort();

    FLAGS_bomb_mark_obsolete = true;
    run_l1();
    rows = cost_rows();
    ASSERT_EQ(rows.size(), kProducts);
    writers.clear();
    for (const auto& [p, row] : rows) {
        EXPECT_EQ(row.obsolete, p == removed);
        writers.insert(row.writer.obj_);
    }
    EXPECT_EQ(writers.size(), 1u);

    EXPECT_EQ(run_s2(), static_cast<size_t>(kProducts - 1));
    const uint64_t skips = InsertOnlySkips.load();
    EXPECT_TRUE(tx_->commit());
    EXPECT_EQ(InsertOnlySkips.load(), skips + 1);

    // Listed products stay unmarked, and L1 keeps rewriting the obsolete row.
    run_l1();
    rows = cost_rows();
    writers.clear();
    for (const auto& [p, row] : rows) {
        EXPECT_EQ(row.obsolete, p == removed);
        writers.insert(row.writer.obj_);
    }
    EXPECT_EQ(writers.size(), 1u);
}

}  // namespace
