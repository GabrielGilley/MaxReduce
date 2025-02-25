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
    db.install_filter("BTC_tx_out_dataset_creation");
    db.process();

    size_t output_count = 0;
    size_t tx_to_output_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx_outputs.csv")) {
            ++output_count;
        }
        if (entry.has_tag("tx_to_outputs.csv")) {
            ++tx_to_output_count;
        }
    }

    EQ(output_count, 2);
    EQ(tx_to_output_count, 2);

    TEST_PASS
}

TEST(large_graph) {
    SeqDB db;
    db.add_db_file(data_dir + "/bitcoin_for_subgraph_search.txt");

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_tx_out_dataset_creation");
    db.process();

    size_t output_count = 0;
    size_t tx_to_output_count = 0;
    size_t tx_to_addr_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx_outputs.csv")) {
            ++output_count;
        }
        if (entry.has_tag("tx_to_outputs.csv")) {
            ++tx_to_output_count;
        }
        if (entry.has_tag("address_to_tx_outputs.csv")) {
            ++tx_to_addr_count;
        }
    }

    EQ(output_count, 38874);
    EQ(tx_to_output_count, 38874);
    EQ(tx_to_addr_count, 38874);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(manual_run)
    RUN_TEST(large_graph)
    elga::ZMQChatterbox::Teardown();
TESTS_END
