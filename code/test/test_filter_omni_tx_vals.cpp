#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;
TEST(omni_values_manual) {
    SeqDB db {data_dir+"/exchange_rates/tether_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("OMNI", "tx").value() = "{\"amount\": \"1995.00000000\", \"block\": 649999, \"blockhash\": \"000000000000000000076c1eea129cec5a5291d0ee516c3305df33b0ba76ac51\", \"blocktime\": 1601076882, \"txid\": \"3aa7e11aff20bd7ad292b14b7c3dd3591a1986a747b24a38e4c56b821e4e8554\"}";
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("OMNI_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool out0_found = false;
    bool usd_out0_found = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_omni")) {
            if (entry.value() == "1995.000000") {
                EQ(out0_found, false);
                out0_found = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            if (entry.value() == "1999.821167") {
                EQ(usd_out0_found, false);
                usd_out0_found = true;
            }
        }
    }

    EQ(out0_found, true);
    EQ(usd_out0_found, true);

    TEST_PASS
}

TEST(omni_values_simple) {
    SeqDB db {data_dir+"/exchange_rates/tether_exchange_rate"};
    db.add_db_file(data_dir+"/simple_omni.txt");

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("OMNI_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_omni_285 = false;
    bool found_omni_546 = false;
    size_t found_usd_286 = false;
    bool found_usd_547 = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_omni")) {
            if (entry.value() == "285.764801") {
                EQ(found_omni_285, false);
                found_omni_285 = true;
            }
            if (entry.value() == "546.000000") {
                EQ(found_omni_546, false);
                found_omni_546 = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            if (entry.value() == "286.455383") {
                EQ(found_usd_286, false);
                found_usd_286 = true; 
            }
            if (entry.value() == "547.319458") {
                EQ(found_usd_547, false);
                found_usd_547  = true;
            }
        }
    }

    EQ(found_omni_285, true);
    EQ(found_omni_546, true);
    EQ(found_usd_286, true);
    EQ(found_usd_547, true);


    TEST_PASS
}

TEST(no_exchange_data) {
    //Should test that even if the exchange rate data for the timestamp cannot
    //be found, it should still terminate
    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("OMNI", "tx").value() = "{\"amount\": \"1995.00000000\", \"block\": 649999, \"blockhash\": \"000000000000000000076c1eea129cec5a5291d0ee516c3305df33b0ba76ac51\", \"blocktime\": 1601076882, \"txid\": \"3aa7e11aff20bd7ad292b14b7c3dd3591a1986a747b24a38e4c56b821e4e8554\"}";
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("OMNI_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    bool found_inactive = false;

    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("tx")) {
            EQ(entry.has_tag("OMNI_tx_vals:inactive"), true);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);

    TEST_PASS
}

TEST(inactive_exchange_rate) {
    //Test that if an exchange rate isn't initially found, the filter runs
     // properly once the exchange rate entry is found

    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("OMNI", "tx").value() = "{\"amount\": \"1995.00000000\", \"block\": 649999, \"blockhash\": \"000000000000000000076c1eea129cec5a5291d0ee516c3305df33b0ba76ac51\", \"blocktime\": 1601076882, \"txid\": \"3aa7e11aff20bd7ad292b14b7c3dd3591a1986a747b24a38e4c56b821e4e8554\"}";
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("OMNI_tx_vals");
    db.install_filter("exchange_rates");
    db.process();

    //Verify that it was marked as inactive before the exchange rate is found
    bool found_inactive = false;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("OMNI_tx_vals:inactive")) {
            EQ(found_inactive, false);
            found_inactive = true;
        }
    }
    EQ(found_inactive, true);

    //Add the exchange rate
    {
        DBEntry<> e;
        e.clear().value() = "100";
        chain_info_t ci = pack_chain_info(USDT_KEY, EXCHANGE_RATE_KEY, 0);
        vtx_t b_key = 1600992000;
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        e.add_tag("USDT");
        db.add_entry(move(e));
    }
    db.stage_close();

    //Run the filter again, making sure it is now run fully
    bool done_found = false;
    size_t usd_count = 0;
    size_t omni_count = 0;
    db.process();
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("OMNI_tx_vals:inactive")){
            TEST_FAIL
        }
        if (entry.has_tag("OMNI_tx_vals:done")) {
            EQ(done_found, false);
            done_found = true;
        }
        if (entry.has_tag("out_val_usd")) {
            usd_count++;
        }
        if (entry.has_tag("out_val_omni")) {
            omni_count++;
        }
    }

    EQ(usd_count, 1);
    EQ(done_found, true);
    EQ(omni_count, 1);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(omni_values_manual)
    RUN_TEST(omni_values_simple)
    RUN_TEST(no_exchange_data)
    RUN_TEST(inactive_exchange_rate)
    elga::ZMQChatterbox::Teardown();
TESTS_END
