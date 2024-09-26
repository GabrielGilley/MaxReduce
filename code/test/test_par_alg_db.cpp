#include "test.hpp"
#include "par_db.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <thread>
#include <map>

using namespace std;
using namespace pando;

localnum_t g_idx = 0;
size_t inc_amount = 3;


TEST(key_duplication) {
    // Create the mesh
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread db1 { db1_addr };

    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread db2 { db2_addr };

    ParDBClient c1 { db1_addr };
    ParDBClient c2 { db2_addr };

    c1.add_neighbor(db2_addr);

    // Ensure it finishes within 50 ms
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 2);
    EQ(c2.num_neighbors(), 2);

    // Add the entries, they should all go to the same DB
    for (vtx_t i = 0; i < 100; i++) {
        dbkey_t key {2, 3, i};
        DBEntry e; e.set_key(key);
        c1.add_entry(e);
    }

    EQ(c1.db_size(), 100);
    EQ(c2.db_size(), 0);
    
    TEST_PASS
}

TEST(message_packing) {
    // Setup a ParDB mesh with 3 agents(?)

    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDB db1 { db1_addr };

    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDB db2 { db2_addr };

    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDB db3 { db3_addr };

    db1.add_neighbor(db2_addr.serialize());
    db1.add_neighbor(db3_addr.serialize());

    db2.recv_msg();
    db1.recv_msg();

    db3.recv_msg();
    db1.recv_msg();

    db3.recv_msg();
    db2.recv_msg();

    EQ(db1.num_neighbors(), 3);
    EQ(db2.num_neighbors(), 3);
    EQ(db3.num_neighbors(), 3);

    // Put together a msgs object
    msg_t msgs;

    map<string, int> check_map;

    const itkey_t iter = 12345;
    for (vtx_t v = 0; v < 15; ++v) {
        for (size_t idx = 0; idx < 5; ++idx) {
            MData md;
            string str = to_string(iter)+"."+to_string(v)+"."+to_string(idx);
            ++check_map[str];
            md.from_string(str);
            msgs[iter][v].push_back(md);
        }
    }

    unordered_map<graph_t, msg_t> graph_msgs;
    graph_msgs[iter] = msgs;

    bool active = false;
    auto organized_msgs = db1.organize_msgs_by_agents(active, iter, graph_msgs);
    EQ(organized_msgs.size(), 3);

    // Actually test the contents(?) or the size(?)
    // TODO test unpacking
    map<vtx_t, int> check_vmap;
    for (auto [agent_id, agent_mdata] : organized_msgs) {
        const char* buf = agent_mdata.get();
        const char* buf_end = buf+agent_mdata.size();

        msg_type_t want_msg;
        bool want_active;
        itkey_t want_iter;
        want_msg = unpack_msg(buf);
        unpack_single(buf, want_active);
        unpack_single(buf, want_iter);
        EQ(want_msg, ALG_INTERNAL_COMPUTE);
        EQ(want_active, active);
        EQ(want_iter, iter);

        auto mdatas = ParDB::split_mdata_to_mdatas(buf, buf_end);
        for (auto & [graph_id, vtx_id, mdata] : mdatas) {
            string str {mdata.get(), mdata.get()+mdata.size()};
            ++check_map[str];
            ++check_vmap[vtx_id];
        }
    }

    EQ(check_vmap.size(), 15);
    for (vtx_t v = 0; v < 15; ++v) {
        EQ(check_vmap[v], 5);
    }

    EQ(check_map.size(), 75);
    for (auto [k, v] : check_map) {
        EQ(v, 2);
    }

    TEST_PASS
}

TEST(par_simple_group_test_1) {
    // Setup a mesh with 1 nodes
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };

    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 1);

    {
        dbkey_t new_key = {1,1,5};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }
    {
        dbkey_t new_key = {1,5,999};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }
    EQ(c1.db_size(), 2);

    c1.add_filter_dir(build_dir+"/test/filters");
    c1.install_filter("test_algdb_simple_group");
    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(50*10));

    EQ(c1.db_size(), 3);

    TEST_PASS
}

TEST(par_simple_group_test_3) {
    // Setup a mesh with 3 nodes
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    {
        dbkey_t new_key = {1,1,5};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }
    {
        dbkey_t new_key = {1,5,999};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }
    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 2);

    c1.add_filter_dir(build_dir+"/test/filters");
    c1.install_filter("test_algdb_simple_group");
    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(50*10));

    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 3);

    TEST_PASS
}


TEST(several_single_one_group) {
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    // Load part of the entry for the group filter. We are going to create two
    // single_entry filters that do the processing that would allow the group
    // filter to run. One of these filters will simply add the 'A' tag to the
    // entry based on it's key. The next filter will see that the 'A' tag was
    // added, which allows it to run, and create the second entry that's
    // expected for the simple_group filter.
    {
        dbkey_t new_key = {1,1,5};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }

    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 1);


    c1.add_filter_dir(build_dir+"/test/filters");

    // purposely do these out of expected order
    c1.install_filter("test_par_alg_db_create_second_entry");
    c1.install_filter("test_par_alg_db_add_first_entry_tag");

    c1.install_filter("test_algdb_simple_group");
    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(50*10));
    
    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 3);

    TEST_PASS
}

TEST(several_single_one_group_single_again) {
    // Very similar to the test several_single_one_group. However, this time
    // we add an extra filter after the group filter. This filter runs on the
    // entry created by the group filter

    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    // Load part of the entry for the group filter. We are going to create two
    // single_entry filters that do the processing that would allow the group
    // filter to run. One of these filters will simply add the 'A' tag to the
    // entry based on it's key. The next filter will see that the 'A' tag was
    // added, which allows it to run, and create the second entry that's
    // expected for the simple_group filter.
    {
        dbkey_t new_key = {1,1,5};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }

    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 1);


    c1.add_filter_dir(build_dir+"/test/filters");

    // purposely install these out of expected order
    c1.install_filter("test_par_alg_db_create_second_entry");
    c1.install_filter("test_par_alg_db_add_first_entry_tag");
    c1.install_filter("test_par_alg_db_after_group");

    c1.install_filter("test_algdb_simple_group");
    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(50*10));

    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 5);

    TEST_PASS
}

TEST(several_single_one_group_one_single_one_group) {
    // Very similar to the test several_single_one_group_single_again.
    // However, this time we add an extra group filter to run after the single
    // filter that's triggered by the group filter. So, the expected order of
    // events would be:
    // single -> single -> group -> single -> group
    // specifically, this is:
    // add_first_entry_tag -> create_second_entry -> simple_group -> after_group -> final_group

    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    // Load part of the entry for the group filter. We are going to create two
    // single_entry filters that do the processing that would allow the group
    // filter to run. One of these filters will simply add the 'A' tag to the
    // entry based on it's key. The next filter will see that the 'A' tag was
    // added, which allows it to run, and create the second entry that's
    // expected for the simple_group filter.
    {
        dbkey_t new_key = {1,1,5};
        DBEntry<> e; e.set_key(new_key); c1.add_entry(e);
    }

    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 1);

    c1.add_filter_dir(build_dir+"/test/filters");

    // purposely install these out of expected order
    c1.install_filter("test_algdb_simple_group");
    c1.install_filter("test_par_alg_db_final_group");
    c1.install_filter("test_par_alg_db_create_second_entry");
    c1.install_filter("test_par_alg_db_add_first_entry_tag");
    c1.install_filter("test_par_alg_db_after_group");

    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(50*50));
    
    EQ(c1.db_size() + c2.db_size() + c3.db_size(), 6);

    TEST_PASS
}

TEST(BTC_tx_subgraph_search_parallel) {
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    vector<ZMQAddress> addrs;
    addrs.push_back(db1_addr);
    addrs.push_back(db2_addr);
    addrs.push_back(db3_addr);

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    c1.add_db_file(data_dir+"/bitcoin_for_subgraph_search.txt");
    while (c1.db_size() + c2.db_size() + c3.db_size() != 1666) {
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    // seed entry for subgraph search
    chain_info_t chain_info = pack_chain_info(BTC_KEY, BFS_INITIAL_VALUE, 0);
    {DBEntry<> e; e.value() = "0"; e.set_key({chain_info, 0x7bd1bfe43ed20b8, 0}); c1.add_entry(e);}

    c1.add_filter_dir(build_dir + "/filters");


    c1.install_filter("BTC_block_to_tx");
    c1.install_filter("BTC_tx_in_edges");
    c1.install_filter("BTC_tx_out_edges");
    c1.install_filter("BTC_tx_subgraph_search");
    this_thread::sleep_for(chrono::milliseconds(50));

    c1.process();

    this_thread::sleep_for(chrono::milliseconds(50));

    wait(addrs);

    bool originator_found = false;
    bool recipient_found = false;

    vector<DBEntry<>> entries;
    auto entries_db1 = c1.query("TX_DISTANCE");
    auto entries_db2 = c2.query("TX_DISTANCE");
    auto entries_db3 = c3.query("TX_DISTANCE");
    move(entries_db1.begin(), entries_db1.end(), std::back_inserter(entries));
    move(entries_db2.begin(), entries_db2.end(), std::back_inserter(entries));
    move(entries_db3.begin(), entries_db3.end(), std::back_inserter(entries));

    for (auto & entry : entries) {
        dbkey_t key = entry.get_key();
        
        if (entry.value() != "18446744073709551615") {
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

TEST(subgraph_search_only) {
    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    /*
    create the tx graph:
    A -> B -> D
    A -> C -> E
         \ -> F
    */

    chain_info_t ci = pack_chain_info(BTC_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=A", "to=B");
        e.value() = "1";
        vtx_t b_key = 10; //A
        vtx_t c_key = 11; //(A->B)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); c1.add_entry(e);
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=A", "to=C");
        e.value() = "2";
        vtx_t b_key = 10; //A
        vtx_t c_key = 12; //(A->C)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); c1.add_entry(e);
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=B", "to=D");
        e.value() = "3";
        vtx_t b_key = 11; //B
        vtx_t c_key = 13; //(B->D)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); c1.add_entry(e);
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=C", "to=E");
        e.value() = "4";
        vtx_t b_key = 12; //C
        vtx_t c_key = 14; //(C->E)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); c1.add_entry(e);
    }
    {
        DBEntry<> e; e.add_tag("BTC", "tx-out-edge", "from=C", "to=F");
        e.value() = "5";
        vtx_t b_key = 12; //C
        vtx_t c_key = 15; //(C->F)
        dbkey_t key {ci, b_key, c_key}; e.set_key(key); c1.add_entry(e);
    }
    chain_info_t chain_info = pack_chain_info(BTC_KEY, BFS_INITIAL_VALUE, 0);
    {DBEntry<> e; e.value() = "0"; e.set_key({chain_info, 10, 0}); c1.add_entry(e);}

    c1.add_filter_dir(build_dir + "/filters");
    c1.install_filter("BTC_tx_subgraph_search");
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(200));

    bool found_a = false;
    bool found_b = false;
    bool found_c = false;
    bool found_d = false;
    bool found_e = false;
    bool found_f = false;

    auto entries_db1 = c1.query("TX_DISTANCE");
    auto entries_db2 = c2.query("TX_DISTANCE");
    auto entries_db3 = c3.query("TX_DISTANCE");

    vector<DBEntry<>> entries;
    move(entries_db1.begin(), entries_db1.end(), std::back_inserter(entries));
    move(entries_db2.begin(), entries_db2.end(), std::back_inserter(entries));
    move(entries_db3.begin(), entries_db3.end(), std::back_inserter(entries));

    for (auto &entry : entries) {
        dbkey_t key = entry.get_key();
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

TEST(in_out_edges) {
    /*
     * There seems to currently be an error with how tx-out-edges and
     * tx-in-edges are processed when run out of order. This test is to help
     * debug that, but also to ensure that it stays correct in the future
     */
    /* An example out edge entry that isn't a UTXO:
    KEY:
    65536:367205904596551596:410305539464313798
    TAGS:
    to=5b1b2b6d9253bc6705ad9f0f2592663a5fe322df6e646ccf7163c59dc9a198f0
    from=51893d31ee747acfac6a1dffcf58d4b0aca05b84043afcc432d3b79f088e215d
    tx-out-edge
    BTC
    VALUE:
    0.080107
    */
    vector<ZMQAddress> addrs;

    elga::ZMQAddress db1_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db1 {db1_addr};
    ParDBClient c1 { db1_addr };
    elga::ZMQAddress db2_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db2 {db2_addr};
    ParDBClient c2 { db2_addr };
    elga::ZMQAddress db3_addr { "127.0.0.1", g_idx+=inc_amount };
    ParDBThread<ParDB> db3 {db3_addr};
    ParDBClient c3 { db3_addr };

    addrs.push_back(db1_addr);
    addrs.push_back(db2_addr);
    addrs.push_back(db3_addr);

    c1.add_neighbor(db2_addr);
    c1.add_neighbor(db3_addr);
    c2.add_neighbor(db3_addr);
    this_thread::sleep_for(chrono::milliseconds(50));

    EQ(c1.num_neighbors(), 3);
    EQ(c2.num_neighbors(), 3);
    EQ(c3.num_neighbors(), 3);

    c1.add_db_file(data_dir+"/bitcoin_for_subgraph_search.txt");
    while (c1.db_size() + c2.db_size() + c3.db_size() != 1666) {
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    c1.add_filter_dir(build_dir + "/filters");
    this_thread::sleep_for(chrono::milliseconds(50));

    c1.install_filter("BTC_block_to_tx");
    c1.install_filter("BTC_tx_in_edges");
    c1.install_filter("BTC_tx_out_edges");

    this_thread::sleep_for(chrono::milliseconds(50));
    c1.process();
    this_thread::sleep_for(chrono::milliseconds(1000));
    wait(addrs);


    // wait for the db size to stabilize
    size_t prev_total_db_size = 9999999999999999; // anything that will make the first check fail
    while (prev_total_db_size != c1.db_size() + c2.db_size() + c3.db_size()) {
        this_thread::sleep_for(chrono::milliseconds(500));
        prev_total_db_size = c1.db_size() + c2.db_size() + c3.db_size();
    }

    vector<DBEntry<>> entries;
    auto entries_db1 = c1.get_entries();
    auto entries_db2 = c2.get_entries();
    auto entries_db3 = c3.get_entries();
    move(entries_db1.begin(), entries_db1.end(), std::back_inserter(entries));
    move(entries_db2.begin(), entries_db2.end(), std::back_inserter(entries));
    move(entries_db3.begin(), entries_db3.end(), std::back_inserter(entries));

    size_t count = 0;
    for (auto & entry : entries) {
        if (entry.has_tag("tx-out-edge") && !entry.has_tag("to=UTXO")) {
            ++count;
        }
    }
    EQ(count > 0, true);
    EQ(count, 11546);

    TEST_PASS

}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();

    RUN_TEST(key_duplication)
    RUN_TEST(message_packing)
    // no SINGLE runs, 1 group run
    RUN_TEST(par_simple_group_test_1)
    RUN_TEST(par_simple_group_test_3)
    // several SINGLE, 1 group
    RUN_TEST(several_single_one_group)
    // several SINGLE, 1 group, then triggers SINGLE again
    RUN_TEST(several_single_one_group_single_again)
    // several SINGLE, 1 group, then triggers SINGLE again, group AGAIN
    RUN_TEST(several_single_one_group_one_single_one_group)
    // TODO Group turns on another group
    // BTC subgraph search
    RUN_TEST(BTC_tx_subgraph_search_parallel)
    RUN_TEST(in_out_edges)
    RUN_TEST(subgraph_search_only)

    elga::ZMQChatterbox::Teardown();
TESTS_END
