#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(exchange_rates) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };
    EQ(db.entries().size(), 2);
    db.add_db_file(data_dir+"/simple_ethereum.txt");
    EQ(db.entries().size(), 4);
    db.add_db_file(data_dir+"/exchange_rates/bitcoin_exchange_rate");
    EQ(db.entries().size(), 2995);
    db.add_db_file(data_dir+"/exchange_rates/ethereum_exchange_rate");

    EQ(db.entries().size(), 5155);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("exchange_rates");
    db.process();
    EQ(db.entries().size(), 10306);

    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("exchange_rate"))
            count++;
    }

    EQ(count, 5151);

    TEST_PASS
}

TEST(token_exchange_rates) {
    /*
     * Load in all token exchange rates and ensure that they are parsed
     * correctly.
     */
    // 2396 entries for all ETL tokens
    SeqDB db { data_dir+"/exchange_rates/token_exchange_rates.txt" };
    EQ(db.entries().size(), 2396);

    db.add_filter_dir(build_dir);
    db.install_filter("exchange_rates");
    db.process();
    // There should be an additional entry for each exchange rate date:
    EQ(db.entries().size(), 2396*2);

    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("exchange_rate"))
            count++;
    }

    EQ(count, 2396);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(exchange_rates)
    RUN_TEST(token_exchange_rates)
    elga::ZMQChatterbox::Teardown();
TESTS_END
