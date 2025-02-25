#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(tag_added) {
    // Load the test data
    SeqDB db { data_dir+"/simple_bitcoin.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BTC_block_to_txtime");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BTC_block_to_txtime:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries())
        if (entry.has_tag("block"))
            EQ(entry.has_tag("BTC_block_to_txtime:done"), true);

    TEST_PASS
}

TEST(tag_not_added) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };
    DBEntry<> n;
    n.add_tag("BTC");
    n.add_tag("x");
    n.add_to_value("some value");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BTC_block_to_txtime");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BTC_block_to_txtime:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("x") || entry.has_tag("txtime"))
            EQ(entry.has_tag("BTC_block_to_txtime:done"), false)
        else
            EQ(entry.has_tag("BTC_block_to_txtime:done"), true)
    }

    TEST_PASS
}

TEST(exclude_run) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BTC_block_to_txtime");

    EQ(db.size(), 2);

    db.process();

    EQ(db.size(), 7);

    db.process();

    EQ(db.size(), 7);

    DBEntry<> e;
    e.add_tag("BTC").add_tag("block").add_to_value("{\"tx\":[{\"txid\":\"12345\"}],\"time\":12345}");
    db.add_entry(e);

    EQ(db.size(), 8);

    db.process();

    EQ(db.size(), 9);

    for (auto & [key, entry] : db.entries()) {
        EQ(entry.has_tag("BTC_block_to_txtime:fail"), false);
    }

    e.add_tag("BTC_block_to_txtime:fail");
    db.add_entry(e);

    EQ(db.size(), 10);

    db.process();

    EQ(db.size(), 10);

    TEST_PASS
}

TEST(bad_json) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };
    DBEntry<> n;
    n.add_tag("BTC");
    n.add_tag("block");
    n.add_tag("invalid-json");
    n.add_to_value("{{not valid JSON");
    db.add_entry(n);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BTC_block_to_txtime");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("BTC_block_to_txtime:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("invalid-json")) {
            EQ(entry.has_tag("BTC_block_to_txtime:done"), false);
            EQ(entry.has_tag("BTC_block_to_txtime:fail"), true);
        } else if (entry.has_tag("txtime")) {
            EQ(entry.has_tag("BTC_block_to_txtime:done"), false);
            EQ(entry.has_tag("BTC_block_to_txtime:fail"), false);
        } else {
            EQ(entry.has_tag("BTC_block_to_txtime:done"), true);
            EQ(entry.has_tag("BTC_block_to_txtime:fail"), false);
        }
    }

    TEST_PASS
}

TEST(txtime_created) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_txtime");
    EQ(db.size(), 2);
    db.process();
    EQ(db.size(), 7);

    size_t block_count = 0;
    size_t t1_count = 0;
    size_t t2_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("block")) {
            ++block_count;
            continue;
        }
        EQ(entry.has_tag("txtime"), true);
        if (entry.value() == "1293623863")
            ++t1_count;
        else if (entry.value() == "1298540131")
            ++t2_count;
    }

    EQ(block_count, 2);
    EQ(t1_count, 4);
    EQ(t2_count, 1);

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
