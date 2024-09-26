#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(build_in_edges) {
    // Create a "partially processed" database, with in edges and transactions
    // Ensure appropriate out edges are built
    SeqDB db;

    // Blockchain:
    //      A -10> B \5
    //        -----9> C
    //        -5>      O1
    //        -1.2345> O2
    DBEntry<> e;

    // Create the transaction JSON
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vin\": [{\"coinbase\": \"12345\" }]}}";
    db.add_entry(e);
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vin\": [{\"txid\": \"A\", \"vout\": 0}]}}";
    db.add_entry(e);
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"C\", \"vin\": [{\"txid\": \"A\", \"vout\": 1}, {\"txid\": \"B\", \"vout\": 0}]}}";
    db.add_entry(e);

    // Load and run the filter
    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("BTC_tx_in_edges");
    db.process();

    bool e_01 = false;
    bool e_12 = false;
    bool e_02 = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("n=0") && string{entry.value()} == "B")
                e_01 = true;
            if (entry.has_tag("from=A") && entry.has_tag("n=1") && string{entry.value()} == "C")
                e_12 = true;
            if (entry.has_tag("from=B") && entry.has_tag("n=0") && string{entry.value()} == "C")
                e_02 = true;
        }
    }

    EQ(e_01, true);
    EQ(e_12, true);
    EQ(e_02, true);

    //Ensure running it again doesn't produce any extra entries
    size_t num_entries = db.entries().size();

    //Ensure the single filter still exists in the DB
    EQ(db.num_filters(), 1);

    db.process();

    EQ(num_entries, db.entries().size());

    TEST_PASS
}

TEST(coinbase) {
    SeqDB db;

    // Create the transaction JSON
    DBEntry<> e;
    e.add_tag("BTC", "txid", "A").value() = "0";
    db.add_entry(e);
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vin\": [{\"coinbase\": \"12345\" }]}}";
    db.add_entry(e);

    // Load and run the filter
    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("BTC_tx_in_edges");
    db.process();

    bool success = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge") && entry.has_tag("from=COINBASE"))
            success = true;
        if (entry.has_tag("BTC_tx_in_edges:fail")) {
            TEST_FAIL
        }
    }

    EQ(success, true);

    TEST_PASS
}

TEST(check_keys) {
    // Create a "partially processed" database, with in edges and transactions
    // Ensure appropriate out edges are built
    SeqDB db;

    // Blockchain:
    //      A -10> B \5
    //        -----9> C
    //        -5>      O1
    //        -1.2345> O2

    //Use real TX IDs for more accurate testing
    string a_key = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4";
    string b_key = "87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03";
    string c_key = "6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4";

    // Create the transaction JSON
    {DBEntry<> e;
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"" + a_key + "\", \"vin\": [{\"coinbase\": \"12345\" }]}}";
    db.add_entry(e);}
    {DBEntry<> e;
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"" + b_key + "\", \"vin\": [{\"txid\": \"" + a_key + "\", \"vout\": 0}]}}";
    db.add_entry(e);}
    {DBEntry<> e;
    e.clear().add_tag("BTC", "tx").value() = "{\"tx\":{\"txid\": \"" + c_key + "\", \"vin\": [{\"txid\": \"" + a_key + "\", \"vout\": 1}, {\"txid\": \"" + b_key + "\", \"vout\": 0}]}}";
    db.add_entry(e);}

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_in_edges");
    db.process();

    bool edge1_found = false;
    bool edge2_found = false;
    bool edge3_found = false;
    chain_info_t expected_ci = pack_chain_info(BTC_KEY, TX_IN_EDGE_KEY, 0);
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            if (entry.has_tag("from=" + a_key) && entry.has_tag("n=0") && string{entry.value()} == b_key) {
                if (edge1_found) TEST_FAIL;
                EQ(entry.get_key().a, expected_ci);
                EQ(entry.get_key().b, 1152680873570800642);
                EQ(entry.get_key().c, 0);
                edge1_found = true;
            }
            if (entry.has_tag("from=" + a_key) && entry.has_tag("n=1") && string{entry.value()} == c_key) {
                if (edge2_found) TEST_FAIL;
                EQ(entry.get_key().a, expected_ci);
                EQ(entry.get_key().b, 1152680873570800642);
                EQ(entry.get_key().c, 1);
                edge2_found = true;
            }
            if (entry.has_tag("from="+b_key) && entry.has_tag("n=0") && string{entry.value()} == c_key) {
                if (edge3_found) TEST_FAIL;
                EQ(entry.get_key().a, expected_ci);
                EQ(entry.get_key().b, 610824335738309319);
                EQ(entry.get_key().c, 0);
                edge3_found = true;
            }
        }
    }

    EQ(edge1_found, true);
    EQ(edge2_found, true);
    EQ(edge3_found, true);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(build_in_edges)
    RUN_TEST(coinbase)
    RUN_TEST(check_keys)
    elga::ZMQChatterbox::Teardown();
TESTS_END
