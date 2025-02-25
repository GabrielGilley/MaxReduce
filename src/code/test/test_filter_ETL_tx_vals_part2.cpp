#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;


TEST(exchange_rates_later) {
    SeqDB db { data_dir+"/ethereum/simple_etl_token_transfers.txt" };
    EQ(db.entries().size(), 3);

    db.add_db_file(data_dir+"/ethereum/simple_etl_tokens.txt");
    EQ(db.entries().size(), 6);

    // Two blocks
    db.add_db_file(data_dir+"/ethereum/simple_ethereum_hacked.txt");
    EQ(db.entries().size(), 8);


    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("exchange_rates");
    db.install_filter("ETH_block_to_txtime");
    db.install_filter("etl_tokens");
    db.install_filter("ETL_tx_vals");

    db.process();

    // After the first round of processing, NOW we add the exchange rates
    db.add_db_file(data_dir+"/exchange_rates/token_exchange_rates.txt");

    db.process();

    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("exchange_rate"))
            count++;
    }

    EQ(count, 2396);

    bool found_b20 = false;
    bool found_b20_usd = false;
    bool found_d07 = false;
    bool found_d07_usd = false;
    bool found_523 = false;
    bool found_523_usd = false;

    size_t etl_count = 0;
    size_t usd_count = 0;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("out_val_etl")) {
            etl_count++;
            if (entry.value() == "59044.96086202055") {
                EQ(found_b20, false);
                found_b20 = true;
            }
            if (entry.value() == "77.3989") {
                EQ(found_d07, false);
                found_d07 = true;
            }
            if (entry.value() == "18.297948185185838") {
                EQ(found_523, false);
                found_523 = true;
            }
        }
        else if (entry.has_tag("out_val_usd")) {
            usd_count++;
            if (entry.value() == "113.733039616") {
                EQ(found_d07_usd, false);
                found_d07_usd = true;
            }
            if (entry.value() == "36861.21661905687") {
                EQ(found_523_usd, false);
                found_523_usd = true;
            }
        }
    }

    // There is no exchange rate for the LOOKS token
    // Side note it was about $73,663.58
    EQ(found_b20, true);
    EQ(found_b20_usd, false);

    EQ(found_d07, true);
    EQ(found_d07_usd, true);

    EQ(found_523, true);
    EQ(found_523_usd, true);

    EQ(etl_count, 3);
    EQ(usd_count, 2);

    TEST_PASS

}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(exchange_rates_later)
    elga::ZMQChatterbox::Teardown();
TESTS_END
