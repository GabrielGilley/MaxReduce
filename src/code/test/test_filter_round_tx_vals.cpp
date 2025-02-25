#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(btc_values_manual) {
    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "out_val_usd").value() = "200.34";
        chain_info_t ci = pack_chain_info(BTC_KEY, OUT_VAL_KEY, 0);
        vtx_t b_key = 1645480436;
        vtx_t c_key = 10; //tx hash
        dbkey_t key = {ci, b_key, c_key};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("round_tx_vals");
    db.process();
    EQ(db.size(), 10);


    //r = rounded val, u = next highest, l = next lowest
    //So, v_r_t_l is value rounded and next lowest timestamp
    bool v_r_t_r = false;
    bool v_r_t_u = false;
    bool v_r_t_l = false;
    bool v_u_t_r = false;
    bool v_u_t_u = false;
    bool v_u_t_l = false;
    bool v_l_t_r = false;
    bool v_l_t_u = false;
    bool v_l_t_l = false;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("rounded")) {
            uint16_t round_key;
            unpack_chain_info(key.a, nullptr, nullptr, &round_key);

            //Value is rounded, timestamp is rounded
            if (key.c == 200 && key.b ==  1645480800) {
                EQ(string{entry.value()}, "10");
                EQ(v_r_t_r, false);
                v_r_t_r = true;
            }
            //Value is rounded, timestamp is next highest
            if (key.c == 200 && key.b ==  1645484400) {
                EQ(string{entry.value()}, "10");
                EQ(v_r_t_u, false);
                v_r_t_u = true;
            }
            //Value is rounded, timestamp is next lowest
            if (key.c == 200 && key.b ==  1645477200) {
                EQ(string{entry.value()}, "10");
                EQ(v_r_t_l, false);
                v_r_t_l = true;
            }
            //Value is next lowest, timestamp is rounded
            if (key.c == 190 && key.b ==  1645480800) {
                EQ(string{entry.value()}, "10");
                EQ(v_l_t_r, false);
                v_l_t_r = true;
            }
            //Value is next lowest, timestamp is next highest
            if (key.c == 190 && key.b ==  1645484400) {
                EQ(string{entry.value()}, "10");
                EQ(v_l_t_u, false);
                v_l_t_u = true;
            }
            //Value is next lowest, timestamp is next lowest
            if (key.c == 190 && key.b ==  1645477200) {
                EQ(string{entry.value()}, "10");
                EQ(v_l_t_l, false);
                v_l_t_l = true;
            }
            //Value is next highest, timestamp is rounded
            if (key.c == 210 && key.b ==  1645480800) {
                EQ(string{entry.value()}, "10");
                EQ(v_u_t_r, false);
                v_u_t_r = true;
            }
            //Value is next highest, timestamp is next highest
            if (key.c == 210 && key.b ==  1645484400) {
                EQ(string{entry.value()}, "10");
                EQ(v_u_t_u, false);
                v_u_t_u = true;
            }
            //Value is next highest, timestamp is next lowest
            if (key.c == 210 && key.b ==  1645477200) {
                EQ(string{entry.value()}, "10");
                EQ(v_u_t_l, false);
                v_u_t_l = true;
            }
        }
    }

    EQ(v_r_t_r, true);
    EQ(v_r_t_u, true);
    EQ(v_r_t_l, true);
    EQ(v_u_t_r, true);
    EQ(v_u_t_u, true);
    EQ(v_u_t_l, true);
    EQ(v_l_t_r, true);
    EQ(v_l_t_u, true);
    EQ(v_l_t_l, true);

    TEST_PASS
}

TEST(association) {
    SeqDB db;
    db.add_db_file(data_dir+"/exchange_rates/bitcoin_exchange_rate");
    db.add_db_file(data_dir+"/exchange_rates/ethereum_exchange_rate");

    string btc_block = "{\
        \"tx\": [\
            {\
                \"txid\": \"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4\",\
                \"hash\": \"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4\",\
                \"vin\": [\
                    {\
                        \"txid\": \"87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03\",\
                        \"vout\": 0\
                    }\
                ],\
                \"vout\": [\
                    {\
                        \"value\": 5.56,\
                        \"n\": 0\
                    },\
                    {\
                        \"value\": 44.44,\
                        \"n\": 1\
                    }\
                ]\
            }\
        ],\
        \"time\": 1582413261\
    }";

    string eth_block = "{\
            \"hash\": \"0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c3\",\
            \"timestamp\": \"0x5e51b5d6\",\
            \"transactions\": [\
                {\
                    \"from\": \"0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01\",\
                    \"hash\": \"0x5b0050024e244e5ca0a5bf1dc49d8874dc1c73a067d9d11f84be53b54f59438a\",\
                    \"to\": \"0x56c830805669f366a7b93411588954e1e1b2aab6\",\
                    \"value\": \"0x57b4f4612a1c040000\"\
                }\
            ]\
        }";

    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "block").value() = btc_block;
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ETH", "block").value() = eth_block;
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_vals");
    db.install_filter("exchange_rates");
    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_block_to_txtime");
    db.install_filter("ETH_tx_vals");
    db.install_filter("round_tx_vals");
    db.process();

    size_t tx_count = 0;
    size_t count = 0;
    size_t match_count = 0;
    string exp_val1 = "1600910407295556\n1152680873570800642";
    string exp_val2 = "1152680873570800642\n1600910407295556";
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("tx")) tx_count++;
        if (entry.has_tag("out_val_usd") && entry.has_tag("rounded") && (entry.has_tag("ETH") || entry.has_tag("BTC"))) {
            count++;
        }
        if (entry.has_tag("MERGED") && (string{entry.value()} == exp_val1 || string{entry.value()} == exp_val2)) {
            match_count++;
        }

    }
    EQ(tx_count, 2);
    EQ(count, 18);
    EQ(match_count, 9);

    TEST_PASS
}

TEST(associate_txs_manual) {
    SeqDB db;
    //1. This pair should match
    //-------------
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578700565, 10};
        e.clear().add_tag("BTC", "out_val_usd").value() = "110.1234";
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578701676, 11};
        e.clear().add_tag("ETH", "out_val_usd").value() = "99.1235";
        e.set_key(key);
        db.add_entry(move(e));
    }
    //-------------
    //2. This pair should not match because of value difference
    //-------------
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578700565, 12};
        e.clear().add_tag("BTC", "out_val_usd").value() = "40.01";
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578701676, 13};
        e.clear().add_tag("ETH", "out_val_usd").value() = "72.11";
        e.set_key(key);
        db.add_entry(move(e));
    }
    //-------------
    //3. This pair should not match because of time difference
    //-------------
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578700565, 14};
        e.clear().add_tag("BTC", "out_val_usd").value() = "1000";
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
        dbkey_t key = {ci, 1578709801, 15};
        e.clear().add_tag("ETH", "out_val_usd").value() = "1000.01";
        e.set_key(key);
        db.add_entry(move(e));
    }
    //-------------
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("round_tx_vals");
    db.process();

    size_t found_count = 0;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("MERGED")) {
            if (string{entry.value()} == "10\n11" || string{entry.value()} == "11\n10") {
                found_count++;
            }
            else TEST_FAIL
        }
    }

    EQ(found_count, 6);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(btc_values_manual)
    RUN_TEST(association)
    RUN_TEST(associate_txs_manual)
    elga::ZMQChatterbox::Teardown();
TESTS_END
