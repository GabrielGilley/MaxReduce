#include "alg_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(subgraph_search_only) {
    AlgDB db;
    /*
    create the tx graph:
    A -> B -> D
    A -> C -> E
         \ -> F
    */

    //FIXME the out-edge filter puts the c_key as the "n" value, so we should
    //update it to be the first bytes of the destination tx to match this
    chain_info_t ci = pack_chain_info(BTC_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=A", "to=B");
        e.value() = "1";
        vtx_t b_key = 10; //A
        vtx_t c_key = 11; //(A->B)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=A", "to=C");
        e.value() = "2";
        vtx_t b_key = 10; //A
        vtx_t c_key = 12; //(A->C)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=B", "to=D");
        e.value() = "3";
        vtx_t b_key = 11; //B
        vtx_t c_key = 13; //(B->D)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=C", "to=E");
        e.value() = "4";
        vtx_t b_key = 12; //C
        vtx_t c_key = 14; //(C->E)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=C", "to=F");
        e.value() = "5";
        vtx_t b_key = 12; //C
        vtx_t c_key = 15; //(C->F)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }

    chain_info_t chain_info = pack_chain_info(BTC_KEY, BFS_INITIAL_VALUE, 0);
    {DBEntry<> e; e.value() = "0"; e.set_key({chain_info, 10, 0}); db.add_entry(move(e));}

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_subgraph_search");
    db.process();

    bool found_a = false;
    bool found_b = false;
    bool found_c = false;
    bool found_d = false;
    bool found_e = false;
    bool found_f = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("TX_DISTANCE")) {
            if (key.b == 10) {
                EQ(found_a, false);
                found_a = true;
                EQ(entry.value(), "0");
            }
            else if (key.b == 11) {
                EQ(found_b, false);
                found_b = true;
                EQ(entry.value(), "1");
            }
            else if (key.b == 12) {
                EQ(found_c, false);
                found_c = true;
                EQ(entry.value(), "1");
            }
            else if (key.b == 13) {
                EQ(found_d, false);
                found_d = true;
                EQ(entry.value(), "2");
            }
            else if (key.b == 14) {
                EQ(found_e, false);
                found_e = true;
                EQ(entry.value(), "2");
            }
            else if (key.b == 15) {
                EQ(found_f, false);
                found_f = true;
                EQ(entry.value(), "2");
            }
            else {
                TEST_FAIL;
            }
        }
    }

    EQ(found_a, true);
    EQ(found_b, true);
    EQ(found_c, true);
    EQ(found_d, true);
    EQ(found_e, true);
    EQ(found_f, true);

    TEST_PASS
}

TEST(subgraph_search_long_chain) {
    AlgDB db;
    /*
    create the tx graph:
    A -> B -> C -> D -> E -> F
    */

    chain_info_t ci = pack_chain_info(BTC_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=A", "to=B");
        vtx_t b_key = 10; //A
        vtx_t c_key = 11; //(A->B)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=B", "to=C");
        vtx_t b_key = 11; //B
        vtx_t c_key = 12; //(B->C)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=C", "to=D");
        vtx_t b_key = 12; //C
        vtx_t c_key = 13; //(C->D)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=D", "to=E");
        vtx_t b_key = 13; //D
        vtx_t c_key = 14; //(D->E)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=E", "to=F");
        vtx_t b_key = 14; //E
        vtx_t c_key = 15; //(E->F)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); db.add_entry(move(e));
    }
    chain_info_t chain_info = pack_chain_info(BTC_KEY, BFS_INITIAL_VALUE, 0);
    {DBEntry<> e; e.value() = "0"; e.set_key({chain_info, 10, 0}); db.add_entry(move(e));}

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_tx_subgraph_search");
    db.process();

    bool found_a = false;
    bool found_b = false;
    bool found_c = false;
    bool found_d = false;
    bool found_e = false;
    bool found_f = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("TX_DISTANCE")) {
            if (key.b == 10) {
                EQ(found_a, false);
                found_a = true;
                EQ(entry.value(), "0");
            }
            else if (key.b == 11) {
                EQ(found_b, false);
                found_b = true;
                EQ(entry.value(), "1");
            }
            else if (key.b == 12) {
                EQ(found_c, false);
                found_c = true;
                EQ(entry.value(), "2");
            }
            else if (key.b == 13) {
                EQ(found_d, false);
                found_d = true;
                EQ(entry.value(), "3");
            }
            else if (key.b == 14) {
                EQ(found_e, false);
                found_e = true;
                EQ(entry.value(), "4");
            }
            else if (key.b == 15) {
                EQ(found_f, false);
                found_f = true;
                EQ(entry.value(), "5");
            }
            else {
                TEST_FAIL;
            }
        }
    }

    EQ(found_a, true);
    EQ(found_b, true);
    EQ(found_c, true);
    EQ(found_d, true);
    EQ(found_e, true);
    EQ(found_f, true);

    TEST_PASS
}

TEST(subgraph_search_all_filters) {
    AlgDB db { data_dir+"/bitcoin_for_subgraph_search.txt", 1ull<<32 };

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.process();
    db.install_filter("BTC_tx_in_edges");
    db.process();
    db.install_filter("BTC_tx_out_edges");
    db.process();
    cout << "done with preprocessing" << endl;

    chain_info_t chain_info = pack_chain_info(BTC_KEY, BFS_INITIAL_VALUE, 0);
    {DBEntry<> e; e.value() = "0"; e.set_key({chain_info, 0x7bd1bfe43ed20b8, 0}); db.add_entry(move(e));}

    db.install_filter("BTC_tx_subgraph_search");
    db.process();

    bool originator_found = false;
    bool recipient_found = false;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("TX_DISTANCE") && entry.value() != "18446744073709551615") {
            if (key.b == 0x7bd1bfe43ed20b8) {
                EQ(originator_found, false);
                originator_found = true;
                EQ(entry.value(), "0");
            }
            else if (key.b == 0xa32ab2d594ba1bc) {
                EQ(recipient_found, false);
                recipient_found = true;
                EQ(entry.value(), "1");
            } else {
                TEST_FAIL
            }
        }
    }

    EQ(originator_found, true);
    EQ(recipient_found, true);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(subgraph_search_only)
    RUN_TEST(subgraph_search_long_chain)
    RUN_TEST(subgraph_search_all_filters)
    elga::ZMQChatterbox::Teardown();
TESTS_END
