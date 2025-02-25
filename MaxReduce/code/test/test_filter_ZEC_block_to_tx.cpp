#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

#include <chrono>

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(tag_added) {
    // Load the test data
    SeqDB db { data_dir + "/zcash_blocks/zcash_blocks_small.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ZEC_block_to_tx:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries())
        if (entry.has_tag("block"))
            EQ(entry.has_tag("ZEC_block_to_tx:done"), true);

    TEST_PASS
}

TEST(tag_not_added) {
    SeqDB db { data_dir + "/zcash_blocks/zcash_blocks_small.txt" };
    DBEntry<> n;
    n.add_tag("ZEC");
    n.add_tag("x");
    n.add_to_value("some value");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ZEC_block_to_tx:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("x") || entry.has_tag("tx"))
            EQ(entry.has_tag("ZEC_block_to_tx:done"), false)
        else
            EQ(entry.has_tag("ZEC_block_to_tx:done"), true)
    }

    TEST_PASS
}

TEST(exclude_run) {
    SeqDB db { data_dir + "/zcash_blocks/zcash_blocks_small.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");

    EQ(db.size(), 208);

    db.process();

    EQ(db.size(), 419);

    db.process();

    EQ(db.size(), 419);

    DBEntry<> e;
    e.add_tag("ZEC").add_tag("block").add_to_value("{\"tx\":[{\"txid\":\"12345\"}]}");
    db.add_entry(e);

    EQ(db.size(), 420);

    db.process();

    EQ(db.size(), 421);

    for (auto & [key, entry] : db.entries()) {
        EQ(entry.has_tag("ZEC_block_to_tx:fail"), false);
    }

    e.add_tag("ZEC_block_to_tx:fail");
    db.add_entry(e);

    EQ(db.size(), 422);

    db.process();

    EQ(db.size(), 422);

    TEST_PASS
}

TEST(bad_json) {
    SeqDB db { data_dir + "/zcash_blocks/zcash_blocks_small.txt" };
    DBEntry<> n;
    n.add_tag("ZEC");
    n.add_tag("block");
    n.add_tag("invalid-json");
    n.add_to_value("{{not valid JSON");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ZEC_block_to_tx:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("invalid-json")) {
            EQ(entry.has_tag("ZEC_block_to_tx:done"), false);
            EQ(entry.has_tag("ZEC_block_to_tx:fail"), true);
        } else if (entry.has_tag("tx")) {
            EQ(entry.has_tag("ZEC_block_to_tx:done"), false);
            EQ(entry.has_tag("ZEC_block_to_tx:fail"), false);
        } else {
            EQ(entry.has_tag("ZEC_block_to_tx:done"), true);
            EQ(entry.has_tag("ZEC_block_to_tx:fail"), false);
        }
    }

    TEST_PASS
}

TEST(tx_created) {
    SeqDB db { data_dir + "/zcash_blocks/zcash_blocks_small.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_block_to_tx");
    EQ(db.size(), 208);
    db.process();
    EQ(db.size(), 419);

    size_t block_count = 0;
    size_t tx_count = 0;
    size_t block_sz = 0;
    size_t tx_sz = 0;
    bool small_found = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("block")) {
            ++block_count;
            block_sz += entry.value().size();
            continue;
        }
        EQ(entry.has_tag("tx"), true);
        ++tx_count;
        tx_sz += entry.value().size();
        if (entry.value().size() == 1860) small_found = true;
    }
    EQ(block_sz, tx_sz + 632794);
    
    EQ(small_found, true);

    EQ(block_count, 208);
    EQ(tx_count, 211);

    TEST_PASS
}


TEST(python_performance) {
    // This doesn't actually test anything, but can be a good metric for comparison to c++ filters
    SeqDB db;

    size_t size = 100;
    for (size_t i = 0; i < size; i++) {
        db.add_db_file( data_dir + "/zcash_blocks/zcash_blocks_small.txt" );
    }
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("python_ZEC_get_block");
    EQ(db.size(), size * 208);

    auto start = high_resolution_clock::now();
    db.process();
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);

    EQ(db.size(), (size * 208) + (size * 211));

    double dur = (double)duration.count() / (double)1000000;
    cout << "python time=" << dur << " seconds" << endl;

    TEST_PASS
}
TEST(cpp_performance) {
    // This doesn't actually test anything, but can be a good metric for comparison to python filters
    SeqDB db;

    size_t size = 100;
    for (size_t i = 0; i < size; i++) {
        db.add_db_file( data_dir + "/zcash_blocks/zcash_blocks_small.txt" );
    }
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_block_to_tx");
    EQ(db.size(), size * 208);
    auto start = high_resolution_clock::now();
    db.process();
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);

    EQ(db.size(), (size * 208) + 211);

    double dur = (double)duration.count() / (double)1000000;
    cout << "cpp time=" << dur << " seconds" << endl;

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(tag_not_added)
    RUN_TEST(exclude_run)
    RUN_TEST(bad_json)
    RUN_TEST(tx_created)
    RUN_TEST(cpp_performance)
    RUN_TEST(python_performance)
    elga::ZMQChatterbox::Teardown();
TESTS_END
