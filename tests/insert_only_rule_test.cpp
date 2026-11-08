// The single-writer insert-only rule, on either engine.
#include <atomic>
#include <string>
#include <vector>

#define GLOBAL_VALUE_DEFINE

#ifdef ENGINE_SAROS
#include "fixture_saros.hh"
#else
#include "fixture_oze.hh"
#endif

namespace {

class InsertOnlyRuleTest : public TestCase {
protected:
    void SetUp() override {
        FLAGS_insert_only_single_writer = true;
#ifdef ENGINE_SAROS
        FLAGS_saros = true;
        FLAGS_validate_insert_only_reads = true;
#endif
        TestCase::SetUp();
        InsertOnlySkips.store(0);
        InsertOnlyMixedAborts.store(0);
        InsertOnlyScanFalls.store(0);
    }

    // Executor `id` updates `keys` and commits.
    void update(const std::string& id, const std::vector<std::string>& keys) {
        begin(id);
        for (const auto& k : keys) ASSERT_TRUE(write(k, id));
        ASSERT_TRUE(commit(id));
    }

    Tuple* index_lookup(const std::string& key) {
        return Masstrees[get_storage(Storage::TestTable)].get_value(key);
    }

    void expect_counters(uint64_t skips, uint64_t mixed, uint64_t falls) {
        EXPECT_EQ(InsertOnlySkips.load(), skips);
        EXPECT_EQ(InsertOnlyMixedAborts.load(), mixed);
        EXPECT_EQ(InsertOnlyScanFalls.load(), falls);
    }
};

// Scan bounds for scan_all(); the engine keeps views of them.
const std::string kLeft = "aaaa";
const std::string kRight = "zzzz";

TEST_F(InsertOnlyRuleTest, SameWriterSkipsReadValidation) {
    update("1", {"a", "b"});
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(1, 0, 0);
    EXPECT_NE(index_lookup("bbbb"), nullptr);
}

TEST_F(InsertOnlyRuleTest, MixedWritersAbort) {
    update("1", {"a"});
    update("3", {"b"});
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_FALSE(commit("2"));
    expect_counters(0, 1, 0);
    EXPECT_EQ(index_lookup("bbbb"), nullptr);
}

TEST_F(InsertOnlyRuleTest, PendingWriterDoesNotMixTheReads) {
    // Executor 1's versions are installed but pending, so the reads skip them.
    begin("1");
    ASSERT_TRUE(write("a", "1"));
    ASSERT_TRUE(write("b", "1"));
    ASSERT_TRUE(txmap[1]->validation());
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(1, 0, 0);
    txmap[1]->writePhase();
}

TEST_F(InsertOnlyRuleTest, ConcurrentScanFallsToFullValidation) {
    begin("3");
    std::vector<TupleBody*> scanned;
    ASSERT_EQ(txmap[3]->scan(Storage::TestTable, kLeft, false, kRight, false, scanned), Status::OK);
    begin("2");
    ASSERT_TRUE(read("c", "2"));
    ASSERT_TRUE(read("d", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(0, 0, 1);
    EXPECT_TRUE(commit("3"));
}

TEST_F(InsertOnlyRuleTest, RuleOffValidatesMixedWriters) {
    FLAGS_insert_only_single_writer = false;
    update("1", {"a"});
    update("3", {"b"});
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(0, 0, 0);
    FLAGS_insert_only_single_writer = true;
}

TEST_F(InsertOnlyRuleTest, TransactionWithoutReadsIsOutsideTheRule) {
    begin("2");
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(0, 0, 0);
}

TEST_F(InsertOnlyRuleTest, UpdatingTransactionIsOutsideTheRule) {
    update("1", {"a"});
    update("3", {"b"});
    begin("2");
    ASSERT_TRUE(read("a", "2"));
    ASSERT_TRUE(read("b", "2"));
    ASSERT_TRUE(write("c", "2"));
    ASSERT_EQ(insert("bbbb", "2"), Status::OK);
    EXPECT_TRUE(commit("2"));
    expect_counters(0, 0, 0);
}

}  // namespace
