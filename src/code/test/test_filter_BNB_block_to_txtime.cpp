#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

#define SIMPLE_BNB_NUM_TX 23
#define SIMPLE_BNB_NUM_BLOCKS 2

TEST(tag_added) {
    // Load the test data
    SeqDB db { data_dir + "/simple_bnb.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BNB_block_to_txtime");
    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS);

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BNB_block_to_txtime:done"), false);
    db.process();
    EQ(db.size(), SIMPLE_BNB_NUM_TX + SIMPLE_BNB_NUM_BLOCKS);

    size_t count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("block")) {
            EQ(entry.has_tag("BNB_block_to_txtime:done"), true);
            count++;
        }
    }
    EQ(count, SIMPLE_BNB_NUM_BLOCKS);

    TEST_PASS
}

TEST(tag_not_added) {
    SeqDB db { data_dir+"/simple_bnb.txt" };
    DBEntry<> n;
    n.add_tag("BNB");
    n.add_tag("x");
    n.add_to_value("some value");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BNB_block_to_txtime");
    
    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + 1);

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BNB_block_to_txtime:done"), false);

    db.process();

    bool found_not_added = false;

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("x") || entry.has_tag("txtime")) {
            EQ(entry.has_tag("BNB_block_to_txtime:done"), false)
            found_not_added = true;
        }
        else
            EQ(entry.has_tag("BNB_block_to_txtime:done"), true)
    }

    EQ(found_not_added, true);

    TEST_PASS
}

TEST(exclude_run) {
    SeqDB db { data_dir+"/simple_bnb.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BNB_block_to_txtime");

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS);

    db.process();

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX);

    db.process();

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX);

    DBEntry<> e;
    e.add_tag("BNB").add_tag("block").add_to_value("{\"transactions\":[{\"hash\":\"0x12345678\"}],\"timestamp\":\"0x56bfb415\"}");
    db.add_entry(e);

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX + 1);

    db.process();

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX + 2);

    for (auto & [key, entry] : db.entries()) {
        EQ(entry.has_tag("BNB_block_to_txtime:fail"), false);
    }

    e.add_tag("BNB_block_to_txtime:fail");
    db.add_entry(e);

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX + 3);

    db.process();

    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX + 3);

    TEST_PASS
}

TEST(bad_json) {
    SeqDB db { data_dir+"/simple_bnb.txt" };
    DBEntry<> n;
    n.add_tag("BNB");
    n.add_tag("block");
    n.add_tag("invalid-json");
    n.add_to_value("{{not valid JSON");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BNB_block_to_txtime");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BNB_block_to_txtime:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("invalid-json")) {
            EQ(entry.has_tag("BNB_block_to_txtime:done"), false);
            EQ(entry.has_tag("BNB_block_to_txtime:fail"), true);
        } else if (entry.has_tag("txtime")) {
            EQ(entry.has_tag("BNB_block_to_txtime:done"), false);
            EQ(entry.has_tag("BNB_block_to_txtime:fail"), false);
        } else {
            EQ(entry.has_tag("BNB_block_to_txtime:done"), true);
            EQ(entry.has_tag("BNB_block_to_txtime:fail"), false);
        }
    }

    TEST_PASS
}

TEST(txtime_created) {
    SeqDB db { data_dir+"/simple_bnb.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BNB_block_to_txtime");
    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS);
    db.process();
    EQ(db.size(), SIMPLE_BNB_NUM_BLOCKS + SIMPLE_BNB_NUM_TX);

    size_t block_count = 0;
    size_t t1_count = 0;
    size_t t2_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("block")) {
            ++block_count;
            continue;
        }
        EQ(entry.has_tag("txtime"), true);
        if (entry.value() == "1484475035")
            ++t1_count;
        else if (entry.value() == "1484588817")
            ++t2_count;
    }

    EQ(block_count, 2);
    EQ(t1_count, 6);
    EQ(t2_count, 17);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(tag_not_added)
    RUN_TEST(exclude_run)
    RUN_TEST(bad_json)
    RUN_TEST(txtime_created)
    elga::ZMQChatterbox::Teardown();
TESTS_END
