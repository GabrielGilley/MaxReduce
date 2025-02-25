#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(build_out_edges) {
    // Create a "partially processed" database, with in edges and transactions
    // Ensure appropriate out edges are built
    SeqDB db;

    // Blockchain:
    //      A -10> B \5
    //        -----9> C
    //        -5>      O1
    //        -1.2345> O2

    // Create the transaction JSON
    {
        DBEntry<> e;
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vout\": [{\"value\": 6.0, \"n\": 0}]}}";
        db.add_entry(move(e));
    }

    // Create the tx IN edges
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=A", "n=0").value() = "B";      // A->B
        dbkey_t key {pack_chain_info(ZEC_KEY, TX_IN_EDGE_KEY, 0), 10, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=A", "n=1").value() = "C";      // A->C
        dbkey_t key {pack_chain_info(ZEC_KEY, TX_IN_EDGE_KEY, 0), 10, 1};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=B", "n=0").value() = "C";      // B->C
        dbkey_t key {pack_chain_info(ZEC_KEY, TX_IN_EDGE_KEY, 0), 11, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    // Make sure we find all five out edges
    bool e_01 = false;
    bool e_12 = false;
    bool e_02 = false;
    bool e_03 = false;
    bool e_04 = false;
    size_t inactive_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-out-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("to=B")) {
                EQ(e_01, false);
                e_01 = true;
                EQ(entry.value(), "10.000000");
            }
            else if (entry.has_tag("from=B") && entry.has_tag("to=C")) {
                EQ(e_12, false);
                e_12 = true;
                EQ(entry.value(), "6.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=C")) {
                EQ(e_02, false);
                e_02 = true;
                EQ(entry.value(), "9.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO") && entry.value() == "5.000000") {
                EQ(e_03, false);
                e_03 = true;
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO")) {
                EQ(e_04, false);
                e_04 = true;
                EQ(entry.value(), "1.234500");
            }
        } else if (entry.has_tag("tx") && entry.has_tag("ZEC_tx_out_edges:inactive")) {
            inactive_count++;
        }
    }
    EQ(e_01, true);
    EQ(e_12, true);
    EQ(e_02, true);
    EQ(e_03, true);
    EQ(e_04, true);
    EQ(inactive_count, 1);

    //Ensure running it again doesn't produce any extra entries
    size_t num_entries = db.entries().size();

    //Ensure the single filter still exists in the DB
    EQ(db.num_filters(), 1);

    db.process();

    EQ(num_entries, db.entries().size());

    TEST_PASS
}

TEST(bad_json) {
    SeqDB db;

    //tx, txid, vout, n

    //e1 should break because it doesn't have the tx key
    DBEntry<> e1;
    e1.add_tag("ZEC", "tx");
    e1.value() = "";
    db.add_entry(e1);

    //e2 should break because it doesn't have the txid key
    DBEntry<> e2;
    e2.add_tag("ZEC", "tx");
    e2.value() = "{\"tx\":{}}";
    db.add_entry(e2);

    //e3 should break because it doesn't have vout
    DBEntry<> e3;
    e3.add_tag("ZEC", "tx");
    e3.value() = "{\"tx\":{\"txid\":\"X1\"}}";
    db.add_entry(e3);

    //e4 should break because it doesn't have an "n" key
    DBEntry<> e4;
    e4.add_tag("ZEC", "tx");
    e4.value() = "{\"tx\":{\"txid\":\"X1\", \"vout\": [\"value\": 10.000000]}}";
    db.add_entry(e4);

    //e5 should break because it doesn't have transaction value
    DBEntry<> e5;
    e5.add_tag("ZEC", "tx");
    e4.value() = "{\"tx\":{\"txid\":\"X1\", \"vout\": [\"n\": 0]}}";
    db.add_entry(e5);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    size_t success_count = 0;
    for (auto& [key, entry] : db.entries() ) {
        if (entry.has_tag("ZEC_tx_out_edges:done")) TEST_FAIL;
        if (entry.has_tag("ZEC_tx_out_edges:fail")) success_count++;
    }

    EQ(success_count, 5);

    TEST_PASS
}

TEST(in_and_out_edges) {
    SeqDB db {};

    // Create the transaction JSON
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}], \"vin\": [{\"coinbase\": \"12345\" }]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vout\": [{\"value\": 5.0, \"n\": 0}], \"vin\": [{\"txid\": \"A\", \"vout\": 0}]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"C\", \"vin\": [{\"txid\": \"A\", \"vout\": 1}, {\"txid\": \"B\", \"vout\": 0}], \"vout\": []}}";
        db.add_entry(e);
    }

    //Test "in" edges
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_in_edges");
    db.process();

    bool e_01 = false;
    bool e_12 = false;
    bool e_02 = false;

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("n=0") && entry.value() == "B")
                e_01 = true;
            if (entry.has_tag("from=A") && entry.has_tag("n=1") && entry.value() == "C")
                e_12 = true;
            if (entry.has_tag("from=B") && entry.has_tag("n=0") && entry.value() == "C")
                e_02 = true;
        }
    }

    EQ(e_01, true);
    EQ(e_12, true);
    EQ(e_02, true);

    //Test "out" edges
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    // Make sure we find all five out edges
    e_01 = false;
    e_12 = false;
    e_02 = false;
    bool e_03 = false;
    bool e_04 = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-out-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("to=B")) {
                EQ(e_01, false);
                e_01 = true;
                EQ(entry.value(), "10.000000");
            }
            else if (entry.has_tag("from=B") && entry.has_tag("to=C")) {
                EQ(e_12, false);
                e_12 = true;
                EQ(entry.value(), "5.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=C")) {
                EQ(e_02, false);
                e_02 = true;
                EQ(entry.value(), "9.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO") && entry.value() == "5.000000") {
                EQ(e_03, false);
                e_03 = true;
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO")) {
                EQ(e_04, false);
                e_04 = true;
                EQ(entry.value(), "1.234500");
            }
        }
    }
    EQ(e_01, true);
    EQ(e_12, true);
    EQ(e_02, true);
    EQ(e_03, true);
    EQ(e_04, true);

    //Ensure running it again doesn't produce any extra entries
    size_t num_entries = db.entries().size();

    //Ensure the single filter still exists in the DB
    EQ(db.num_filters(), 2);

    db.process();

    EQ(num_entries, db.entries().size());

    TEST_PASS
}

TEST(in_and_out_edges_unordered) {
    //Same as in_and_out_edges, but this time we run the filters all together so that order is not guaranteed
    SeqDB db {};

    // Create the transaction JSON
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"A\", \"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}], \"vin\": [{\"coinbase\": \"12345\" }]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vout\": [{\"value\": 5.0, \"n\": 0}], \"vin\": [{\"txid\": \"A\", \"vout\": 0}]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e; e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"C\", \"vin\": [{\"txid\": \"A\", \"vout\": 1}, {\"txid\": \"B\", \"vout\": 0}], \"vout\": []}}";
        db.add_entry(e);
    }

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_out_edges");
    db.install_filter("ZEC_tx_in_edges");
    db.process();

    bool e_01_in = false;
    bool e_12_in = false;
    bool e_02_in = false;

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("n=0") && entry.value() == "B")
                e_01_in = true;
            if (entry.has_tag("from=A") && entry.has_tag("n=1") && entry.value() == "C")
                e_12_in = true;
            if (entry.has_tag("from=B") && entry.has_tag("n=0") && entry.value() == "C")
                e_02_in = true;
        }
    }

    EQ(e_01_in, true);
    EQ(e_12_in, true);
    EQ(e_02_in, true);


    // Make sure we find all five out edges
    bool e_01_out = false;
    bool e_12_out = false;
    bool e_02_out = false;
    bool e_03_out = false;
    bool e_04_out = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-out-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("to=B")) {
                EQ(e_01_out, false);
                e_01_out = true;
                EQ(entry.value(), "10.000000");
            }
            else if (entry.has_tag("from=B") && entry.has_tag("to=C")) {
                EQ(e_12_out, false);
                e_12_out = true;
                EQ(entry.value(), "5.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=C")) {
                EQ(e_02_out, false);
                e_02_out = true;
                EQ(entry.value(), "9.000000");
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO") && entry.value() == "5.000000") {
                EQ(e_03_out, false);
                e_03_out = true;
            }
            else if (entry.has_tag("from=A") && entry.has_tag("to=UTXO") && entry.value() == "1.234500") {
                EQ(e_04_out, false);
                e_04_out = true;
            }
        }
    }
    EQ(e_01_out, true);
    EQ(e_12_out, true);
    EQ(e_02_out, true);
    EQ(e_03_out, true);
    EQ(e_04_out, true);

    //Ensure running it again doesn't produce any extra entries
    size_t num_entries = db.entries().size();

    //Ensure the single filter still exists in the DB
    EQ(db.num_filters(), 2);

    db.process();

    EQ(num_entries, db.entries().size());
    TEST_PASS
}

TEST(in_edge_nodata) {
    //Testing when an input is not found

    //Sample chain:
    //A -> B -> D
    //A -> C -> D
    //We test by removing A, and check the results of B, C and D

    SeqDB db {};

    { DBEntry<> e; e.clear().add_tag("ZEC", "txid", "B").value() = "1"; db.add_entry(e); }
    { DBEntry<> e; e.clear().add_tag("ZEC", "txid", "C").value() = "2"; db.add_entry(e); }
    { DBEntry<> e; e.clear().add_tag("ZEC", "txid", "D").value() = "3"; db.add_entry(e); }

    // Create the transaction JSON
    {
        DBEntry<> e;
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"B\", \"vout\": [{\"value\": 7.0, \"n\": 0}, {\"value\": 8.0, \"n\": 1}],"
                "\"vin\": [{\"txid\": \"A\", \"vout\": 0 }]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"C\", \"vout\": [{\"value\": 6.0, \"n\": 0}],"
                "\"vin\": [{\"txid\": \"A\", \"vout\": 1}]}}";
        db.add_entry(e);
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"D\", \"vout\": [{\"value\": 7.0, \"n\": 0}],"
                "\"vin\": [{\"txid\": \"B\", \"vout\": 0 },"
                "{\"txid\": \"C\", \"vout\": 0}]}}";
        db.add_entry(e);
    }

    //Test "in" edges
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_in_edges");
    db.process();

    //Test "out" edges
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    // Make sure we find all out edges
    bool e_out_bd = false;
    bool e_out_bU = false;
    bool e_out_cd = false;
    bool e_out_dU = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-out-edge")) {
            if (entry.has_tag("from=B") && entry.has_tag("to=D")) {
                EQ(e_out_bd, false);
                e_out_bd = true;
                EQ(entry.value(), "7.000000");
            } else if (entry.has_tag("from=B") && entry.has_tag("to=UTXO")) {
                EQ(e_out_bU, false);
                e_out_bU = true;
                EQ(entry.value(), "8.000000");
            } else if (entry.has_tag("from=C") && entry.has_tag("to=D")) {
                EQ(e_out_cd, false);
                e_out_cd = true;
                EQ(entry.value(), "6.000000");
            } else if (entry.has_tag("from=D") && entry.has_tag("to=UTXO")) {
                EQ(e_out_dU, false);
                e_out_dU = true;
                EQ(entry.value(), "7.000000");
            } else
                TEST_FAIL
        }
    }

    EQ(e_out_bd, true);
    EQ(e_out_bU, true);
    EQ(e_out_cd, true);
    EQ(e_out_dU, true);

    // Make sure we find all in edges
    bool e_in_ab = false;
    bool e_in_ac = false;
    bool e_in_bd = false;
    bool e_in_cd = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            if (entry.has_tag("from=A") && entry.has_tag("n=0")) {
                EQ(e_in_ab, false);
                e_in_ab = true;
                EQ(entry.value(), "B");
            } else if (entry.has_tag("from=A") && entry.has_tag("n=1")) {
                EQ(e_in_ac, false);
                e_in_ac = true;
                EQ(entry.value(), "C");
            } else if (entry.has_tag("from=B") && entry.has_tag("n=0")) {
                EQ(e_in_bd, false);
                e_in_bd = true;
                EQ(entry.value(), "D");
            } else if (entry.has_tag("from=C") && entry.has_tag("n=0")) {
                EQ(e_in_cd, false);
                e_in_cd = true;
                EQ(entry.value(), "D");
            }
        }
    }

    EQ(e_in_ab, true);
    EQ(e_in_ac, true);
    EQ(e_in_bd, true);
    EQ(e_in_cd, true);

    TEST_PASS
}

TEST(check_keys) {
    SeqDB db;
    {
        DBEntry<> e; //A
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87\", "
            "\"vout\": [{\"value\": 10.0, \"n\": 0}, {\"value\": 9.0, \"n\": 1}, {\"value\": 5.0, \"n\": 2}, {\"value\": 1.2345,\"n\": 3}]}}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e; //B
        e.clear().add_tag("ZEC", "tx").value() = "{\"tx\":{\"txid\": \"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4\", "
            "\"vout\": [{\"value\": 6.0, \"n\": 0}]}}";
        db.add_entry(move(e));
    }

    // Create the tx IN edges
    chain_info_t ci = pack_chain_info(ZEC_KEY, TX_IN_EDGE_KEY, 0);
    {
        // A->B
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", "n=0");
        e.value() = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4";
        dbkey_t key {ci, 0x8c14f0db3df1501, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        // A->C
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", "n=1");
        e.value() = "87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03";
        dbkey_t key {ci, 0x8c14f0db3df1501, 1};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        // B->C
        DBEntry<> e; e.clear().add_tag("ZEC", "tx-in-edge", "from=fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", "n=0");
        e.value() = "87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03";
        dbkey_t key {ci, 0xfff2525b8931402, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }

    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    bool e_01 = false;
    bool e_12 = false;
    bool e_02 = false;
    bool e_03 = false;
    bool e_04 = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-out-edge")) {
            uint32_t blockchain;
            uint16_t edge_type;
            uint16_t flags;
            unpack_chain_info(key.a, &blockchain, &edge_type, &flags);
            EQ(blockchain, ZEC_KEY);
            EQ(edge_type, TX_OUT_EDGE_KEY);
            if (entry.has_tag("from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
                && entry.has_tag("to=fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")) {
                    EQ(e_01, false);
                    e_01 = true;
                    EQ(key.b, 0x8c14f0db3df1501);
                    EQ(key.c, 0xfff2525b8931402);
                    EQ(flags, NOT_UTXO_KEY);
            }
            else if (entry.has_tag("from=fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
                && entry.has_tag("to=87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03")) {
                    EQ(e_12, false);
                    e_12 = true;
                    EQ(key.b, 0xfff2525b8931402);
                    EQ(key.c, 0x87a157f3fd88ac7);
                    EQ(flags, NOT_UTXO_KEY);
            }
            else if (entry.has_tag("from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
                && entry.has_tag("to=87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03")) {
                    EQ(e_02, false);
                    e_02 = true;
                    EQ(key.b, 0x8c14f0db3df1501);
                    EQ(key.c, 0x87a157f3fd88ac7);
                    EQ(flags, NOT_UTXO_KEY);
            }
            else if (entry.has_tag("from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
                && entry.has_tag("to=UTXO") && entry.value() == "5.000000") {
                    EQ(e_03, false);
                    e_03 = true;
                    EQ(flags, UTXO_KEY);
                    EQ(key.b, 0x8c14f0db3df1501);
                    EQ(key.c, 2);
            }
            else if (entry.has_tag("from=8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
                && entry.has_tag("to=UTXO")) {
                    EQ(e_04, false);
                    e_04 = true;
                    EQ(flags, UTXO_KEY);
                    EQ(key.b, 0x8c14f0db3df1501);
                    EQ(key.c, 3);
            }
        }
    }
    EQ(e_01, true);
    EQ(e_12, true);
    EQ(e_02, true);
    EQ(e_03, true);
    EQ(e_04, true);

    TEST_PASS

}

TEST(check_inactive) {
    cout << "Not yet implemented" << endl;
    TEST_FAIL
    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(build_out_edges)
    RUN_TEST(bad_json)
    RUN_TEST(in_and_out_edges)
    RUN_TEST(in_and_out_edges_unordered)
    RUN_TEST(in_edge_nodata)
    RUN_TEST(check_keys)
    elga::ZMQChatterbox::Teardown();
TESTS_END
