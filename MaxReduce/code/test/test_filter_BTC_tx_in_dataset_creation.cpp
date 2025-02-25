#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(manual_run) {
    SeqDB db;

    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vin\": [], \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1} ]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vin\": [{\"txid\": \"A\", \"vout\": 0}, {\"txid\": \"A\", \"vout\": 1}], \"vout\": []}}";
        db.add_entry(move(e));
    }

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_vals");
    db.install_filter("BTC_tx_in_edges");
    db.install_filter("BTC_tx_out_edges");
    db.install_filter("BTC_tx_in_dataset_creation");
    db.process();

    size_t input_count = 0;
    size_t tx_to_input_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx_inputs.csv")) {
            ++input_count;
        }
        if (entry.has_tag("tx_to_inputs.csv")) {
            ++tx_to_input_count;
        }
    }

    EQ(input_count, 2);
    EQ(tx_to_input_count, 2);

    TEST_PASS
}

TEST(large_graph) {
    SeqDB db;
    db.add_db_file(data_dir + "/bitcoin_for_subgraph_search.txt");

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_tx_vals");
    db.install_filter("BTC_tx_in_edges");
    db.install_filter("BTC_tx_in_dataset_creation");
    db.process();
    db.install_filter("BTC_vout_addrs"); // enforce this to run out of order as an extra test
    db.process();

    size_t input_count = 0;
    size_t tx_to_input_count = 0;
    size_t tx_to_addr_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx_inputs.csv")) {
            ++input_count;
        }
        if (entry.has_tag("tx_to_inputs.csv")) {
            ++tx_to_input_count;
        }
        if (entry.has_tag("address_to_tx_inputs.csv")) {
            ++tx_to_addr_count;
        }
    }

    EQ(input_count, 13276);
    EQ(tx_to_input_count, 13276);
    EQ(tx_to_addr_count, 8490);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(manual_run)
    RUN_TEST(large_graph)
    elga::ZMQChatterbox::Teardown();
TESTS_END
