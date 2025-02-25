#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(eth_values_manual) {
    SeqDB db {data_dir+"/exchange_rates/ethereum_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("ETH", "tx").value() = "{\"transactions\": {\"hash\": \"A\", \"value\": \"0xde126fc2d71df79\"}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ETH", "tx").value() = "{\"transactions\": {\"hash\": \"B\", \"value\": \"0x44b1eec6162f0000\"}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "1588114800";
        chain_info_t ci = pack_chain_info(ETH_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().value() = "1439168151";
        chain_info_t ci = pack_chain_info(ETH_KEY, TXTIME_KEY, 0);
        vtx_t b_key = 11; //B
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ETH_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool a_found = false;
    bool b_found = false;
    bool usd_a_found = false;
    bool usd_b_found = false;
    
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_eth")) {
            if (entry.value() == "4.950000") {
                EQ(b_found, false);
                b_found = true;
            }
            if (entry.value() == "1.000123") {
                EQ(a_found, false);
                a_found = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            if (entry.value() == "198.811086") {
                EQ(usd_b_found, false);
                usd_b_found = true;
            }
            if (entry.value() == "3.612777") {
                EQ(usd_a_found, false);
                usd_a_found = true;
            }
        }
    }
    EQ(a_found, true);
    EQ(b_found, true);
    EQ(usd_a_found, true);
    EQ(usd_b_found, true);

    TEST_PASS
}

TEST(eth_values_simple) {
    SeqDB db {data_dir+"/exchange_rates/ethereum_exchange_rate"};
    db.add_db_file(data_dir+"/simple_ethereum.txt");

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_block_to_txtime");
    db.install_filter("ETH_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_e0b = false;
    bool found_e0b_usd = false;
    bool found_44b = false;
    bool found_44b_usd = false;
    size_t eth_count = 0;
    size_t usd_count = 0;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_eth")) {
            eth_count++;
            if (entry.value() == "1.011962") {
                EQ(found_e0b, false);
                found_e0b = true;
            }
            if (entry.value() == "4.950000") {
                EQ(found_44b, false);
                found_44b = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            usd_count++;
            if (entry.value() == "10.119236") {
                EQ(found_e0b_usd, false);
                found_e0b_usd = true;
            }
            if (entry.value() == "49.498123") {
                EQ(found_44b_usd, false);
                found_44b_usd = true;
            }
        }
    }

    EQ(found_e0b, true);
    EQ(found_44b, true);
    EQ(found_e0b_usd, true);
    EQ(found_44b_usd, true);
    EQ(eth_count, 23);
    EQ(usd_count, eth_count);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(eth_values_manual)
    //RUN_TEST(eth_values_simple) FIXME don't have exchange rate data old enough for this yet
    elga::ZMQChatterbox::Teardown();
TESTS_END
