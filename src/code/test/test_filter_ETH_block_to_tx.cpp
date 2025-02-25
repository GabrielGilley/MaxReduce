#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

#define SIMPLE_ETH_NUM_TX 23
#define SIMPLE_ETH_NUM_BLOCKS 2

TEST(tag_added) {
    // Load the test data
    SeqDB db { data_dir+"/simple_ethereum.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ETH_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ETH_block_to_tx:done"), false);

    db.process();

    size_t num_passes = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("block")) {
            EQ(entry.has_tag("ETH_block_to_tx:done"), true);
            num_passes++;
        }
    }
    EQ(num_passes, SIMPLE_ETH_NUM_BLOCKS);

    TEST_PASS
}

TEST(tag_not_added) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };
    DBEntry<> n;
    n.add_tag("ETH");
    n.add_tag("x");
    n.add_to_value("some value");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ETH_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ETH_block_to_tx:done"), false);

    db.process();
    EQ(db.entries().size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS + 1);

    size_t num_success = 0;
    size_t num_failures = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("x") || entry.has_tag("tx")) {
            EQ(entry.has_tag("ETH_block_to_tx:done"), false)
            num_failures++;
        }
        else {
            EQ(entry.has_tag("ETH_block_to_tx:done"), true)
            num_success++;
        }
    }

    EQ(num_failures, SIMPLE_ETH_NUM_TX + 1);
    EQ(num_success, SIMPLE_ETH_NUM_BLOCKS);

    TEST_PASS
}

TEST(exclude_run) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ETH_block_to_tx");

    EQ(db.size(), 2);

    db.process();

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS);

    db.process();

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS);

    DBEntry<> e;
    e.add_tag("ETH").add_tag("block").add_to_value("{\"transactions\":[{\"hash\":\"0x12345\"}]}");
    db.add_entry(e);

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS + 1);

    db.process();

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS + 2);

    for (auto & [key, entry] : db.entries()) {
        EQ(entry.has_tag("ETH_block_to_tx:fail"), false); //FIXME
    }

    e.add_tag("ETH_block_to_tx:fail");
    db.add_entry(e);

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS + 3);

    db.process();

    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS + 3);

    TEST_PASS
}

TEST(bad_json) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };
    DBEntry<> n;
    n.add_tag("ETH");
    n.add_tag("block");
    n.add_tag("invalid-json");
    n.add_to_value("{{not valid JSON");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ETH_block_to_tx");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ETH_block_to_tx:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("invalid-json")) {
            EQ(entry.has_tag("ETH_block_to_tx:done"), false);
            EQ(entry.has_tag("ETH_block_to_tx:fail"), true);
        } else if (entry.has_tag("tx")) {
            EQ(entry.has_tag("ETH_block_to_tx:done"), false);
            EQ(entry.has_tag("ETH_block_to_tx:fail"), false);
        } else {
            EQ(entry.has_tag("ETH_block_to_tx:done"), true);
            EQ(entry.has_tag("ETH_block_to_tx:fail"), false);
        }
    }

    TEST_PASS
}

TEST(tx_created) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ETH_block_to_tx");
    EQ(db.size(), SIMPLE_ETH_NUM_BLOCKS);
    db.process();
    EQ(db.size(), SIMPLE_ETH_NUM_TX + SIMPLE_ETH_NUM_BLOCKS);

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
        if (entry.value().size() == 881) small_found = true;
    }
    EQ(block_sz, 18521);
    EQ(small_found, true);

    EQ(block_count, SIMPLE_ETH_NUM_BLOCKS);
    EQ(tx_count, SIMPLE_ETH_NUM_TX);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(tag_not_added)
    RUN_TEST(exclude_run)
    RUN_TEST(bad_json)
    RUN_TEST(tx_created)
    elga::ZMQChatterbox::Teardown();
TESTS_END
