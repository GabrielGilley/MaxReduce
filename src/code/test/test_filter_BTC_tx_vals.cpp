#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(btc_values_manual) {
    SeqDB db {data_dir+"/exchange_rates/bitcoin_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "1607212800";
        chain_info_t ci = pack_chain_info(BTC_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool out0_found = false;
    bool out1_found = false;
    bool out2_found = false;
    bool out3_found = false;
    bool usd_out0_found = false;
    bool usd_out1_found = false;
    bool usd_out2_found = false;
    bool usd_out3_found = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_btc")) {
            if (entry.value() == "10.000000") {
                EQ(out0_found, false);
                out0_found = true;
            }
            if (entry.value() == "9.000000") {
                EQ(out1_found, false);
                out1_found = true;
            }
            if (entry.value() == "5.000000") {
                EQ(out2_found, false);
                out2_found = true;
            }
            if (entry.value() == "1.234500") {
                EQ(out3_found, false);
                out3_found = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            if (entry.value() == "193905.000000") {
                EQ(usd_out0_found, false);
                usd_out0_found = true;
            }
            if (entry.value() == "174514.500000") {
                EQ(usd_out1_found, false);
                usd_out1_found = true;
            }
            if (entry.value() == "96952.500000") {
                EQ(usd_out2_found, false);
                usd_out2_found = true;
            }
            if (entry.value() == "23937.572266") {
                EQ(usd_out3_found, false);
                usd_out3_found = true;
            }
        }
    }

    EQ(out0_found, true);
    EQ(out1_found, true);
    EQ(out2_found, true);
    EQ(out3_found, true);
    EQ(usd_out0_found, true);
    EQ(usd_out1_found, true);
    EQ(usd_out2_found, true);
    EQ(usd_out3_found, true);

    TEST_PASS
}

TEST(btc_values_simple) {
    SeqDB db;// {data_dir+"/exchange_rates/bitcoin_exchange_rate"};
    db.add_db_file(data_dir+"/simple_bitcoin.txt");

    //NOTE: These are not real exchange rates for these days.At time of
    //test creation, we didn't have this day's exchange rate so we made it
    //up
    {
        DBEntry<> e; e.clear().add_tag("BTC", "exchange_rate_raw", "2010-12-29");
        e.value() = "147.48800659179688"; db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.clear().add_tag("BTC", "exchange_rate_raw", "2011-02-24");
        e.value() = "147.48800659179688"; db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_btc_44 = false;
    bool found_btc_5 = false;
    size_t found_usd_7374 = 0;
    bool found_usd_6554 = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_btc")) {
            if (entry.value() == "44.439999") {
                EQ(found_btc_44, false);
                found_btc_44 = true;
            }
            if (entry.value() == "5.560000") {
                EQ(found_btc_5, false);
                found_btc_5 = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            if (entry.value() == "7374.400391") {
                found_usd_7374++; 
            }
            if (entry.value() == "6554.366699") {
                EQ(found_usd_6554, false);
                found_usd_6554  = true;
            }
        }
    }

    EQ(found_btc_44, true);
    EQ(found_btc_5, true);
    EQ(found_usd_7374, 2);
    EQ(found_usd_6554, true);


    TEST_PASS
}

TEST(no_exchange_data) {
    //Should test that even if the exchange rate data for the timestamp cannot
    //be found, it should still terminate
    SeqDB db {data_dir+"/exchange_rates/bitcoin_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "946710000";
        e.add_tag("BTC", "txtime", "A");
        chain_info_t ci = pack_chain_info(BTC_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_inactive = false;

    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("tx")) {
            EQ(entry.has_tag("BTC_tx_vals:inactive"), true);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);

    TEST_PASS
}

TEST(no_time_data) {
    //Should test that even if the txtime cannot be found, it should still terminate
    SeqDB db {data_dir+"/exchange_rates/bitcoin_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_inactive = false;

    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("tx")) {
            EQ(entry.has_tag("BTC_tx_vals:inactive"), true);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);

    TEST_PASS
}

TEST(inactive_exchange_rate) {
    /*Test that if an exchange rate isn't initially found, the filter runs
     * properly once the exchange rate entry is found*/
    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "1607212800";
        chain_info_t ci = pack_chain_info(BTC_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    //Verify that it was marked as inactive before the exchange rate is found
    bool found_inactive = false;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("BTC_tx_vals:inactive")) {
            EQ(found_inactive, false);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);

    //Add the exchange rate
    {
        DBEntry<> e;
        e.clear().value() = "100";
        chain_info_t ci = pack_chain_info(BTC_KEY, EXCHANGE_RATE_KEY, 0);
        vtx_t b_key = 1607212800;
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    db.stage_close();

    //Run the filter again, making sure it is now run fully
    bool done_found = false;
    size_t usd_count = 0;
    size_t btc_count = 0;
    db.process();
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("BTC_tx_vals:inactive"))
            TEST_FAIL
        if (entry.has_tag("BTC_tx_vals:done")) {
            EQ(done_found, false);
            done_found = true;
        }
        if (entry.has_tag("out_val_usd")) {
            usd_count++;
        }
        if (entry.has_tag("out_val_btc")) {
            btc_count++;
        }
    }

    EQ(usd_count, 4);
    EQ(done_found, true);
    EQ(btc_count, 4);

    TEST_PASS
}

TEST(inactive_timestamp) {
    /*Test that if a txtime isn't initially found, the filter runs
     * properly once the txtime entry is found*/
    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "100";
        chain_info_t ci = pack_chain_info(BTC_KEY, EXCHANGE_RATE_KEY, 0);
        vtx_t b_key = 1607212800;
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    //Verify that it was marked as inactive before the exchange rate is found
    bool found_inactive = false;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("BTC_tx_vals:inactive")) {
            EQ(found_inactive, false);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);
    
    //Add the txtime
    {
        DBEntry<> e;
        e.clear().value() = "1607212800";
        chain_info_t ci = pack_chain_info(BTC_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }

    db.stage_close();

    //Run the filter again, making sure it is now run fully
    bool done_found = false;
    size_t usd_count = 0;
    size_t btc_count = 0;
    db.process();
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("BTC_tx_vals:inactive"))
            TEST_FAIL
        if (entry.has_tag("BTC_tx_vals:done")) {
            EQ(done_found, false);
            done_found = true;
        }
        if (entry.has_tag("out_val_usd")) {
            usd_count++;
        }
        if (entry.has_tag("out_val_btc")) {
            btc_count++;
        }
    }

    EQ(usd_count, 4);
    EQ(done_found, true);
    EQ(btc_count, 4);

    TEST_PASS
}

TEST(no_exchange_rate_only_btc) {
    SeqDB db;
    db.add_db_file(data_dir+"/simple_bitcoin.txt");

    EQ(db.size(), 2);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_vals");
    db.process();

    size_t out_btc_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("out_val_btc")) {
            ++out_btc_count;
        }
    }

    EQ(out_btc_count, 7);

    TEST_PASS
}

TEST(no_timestamp) {
    SeqDB db;
    db.add_db_file(data_dir+"/simple_bitcoin.txt");

    EQ(db.size(), 2);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_tx_vals");
    db.process();

    size_t out_btc_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("out_val_btc")) {
            ++out_btc_count;
        }
    }

    EQ(out_btc_count, 7);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(btc_values_manual)
    RUN_TEST(btc_values_simple)
    RUN_TEST(no_exchange_data)
    RUN_TEST(no_time_data)
    RUN_TEST(inactive_exchange_rate)
    RUN_TEST(inactive_timestamp)
    RUN_TEST(no_exchange_rate_only_btc)
    //TODO test that if timestamp doesn't exist, btc val still gets made
    RUN_TEST(no_timestamp)
    elga::ZMQChatterbox::Teardown();
TESTS_END
