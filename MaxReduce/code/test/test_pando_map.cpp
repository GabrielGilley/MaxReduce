#include "test.hpp"
#include "pando_map.hpp"
#include "pando_map_client.hpp"
#include "par_db.hpp"
#include "par_db_thread.hpp"
#include <chrono>
#include <thread>
#include <boost/multiprecision/cpp_int.hpp>


using namespace std;
using namespace pando;

localnum_t g_idx = 0;

TEST(insert_retrieve) {
    ZMQAddress addr {"127.0.0.1", ++g_idx};
    PandoMap m {addr};

    DBEntry<pando::PandoMap::Alloc> e {m.get_allocator()};
    e.value() = "testing123";
    dbkey_t k {2,2,3};
    e.set_key(k);
    e.add_tag("test1");
    e.add_tag("test2");

    m.insert(move(e));

    DBEntry<pando::PandoMap::Alloc>* e2 = m.retrieve(k);
    EQ(e2->get_key(), k);
    EQ(e2->value(), "testing123");
    EQ(e2->has_tag("test1"), true);
    EQ(e2->has_tag("test2"), true);
    EQ(e2->tag_size(), 2);

    //make sure querying twice also works
    DBEntry<pando::PandoMap::Alloc>* e3 = m.retrieve(k);
    EQ(e3->get_key(), k);
    EQ(e3->value(), "testing123");
    EQ(e3->has_tag("test1"), true);
    EQ(e3->has_tag("test2"), true);
    EQ(e3->tag_size(), 2);


    TEST_PASS
}


TEST(insert_retrieve_size_ipc) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    EQ(c.size(), 0);

    DBEntry e;
    e.value() = "testing123";
    dbkey_t k {2,2,3};
    e.set_key(k);
    e.add_tag("test1");
    e.add_tag("test2");

    c.insert(move(e));
    EQ(c.size(), 1);

    DBEntry e_ret = c.retrieve(k);

    dbkey_t key_ret_exp {2,2,3};
    EQ(e_ret.value(), "testing123");
    EQ(e_ret.has_tag("test1"), true);
    EQ(e_ret.has_tag("test2"), true);
    EQ(e_ret.get_key(), key_ret_exp);

    DBEntry e2;
    e2.value() = "testing321";
    dbkey_t k2 {3,3,4};
    e2.set_key(k2);
    e2.add_tag("test3");
    e2.add_tag("test4");

    c.insert(move(e2));
    EQ(c.size(), 2);

    DBEntry e_ret2 = c.retrieve(k2);

    dbkey_t key_ret_exp2 {3,3,4};
    EQ(e_ret2.value(), "testing321");
    EQ(e_ret2.has_tag("test3"), true);
    EQ(e_ret2.has_tag("test4"), true);
    EQ(e_ret2.get_key(), key_ret_exp2);

    TEST_PASS
}

TEST(key_exist_ipc) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    EQ(c.size(), 0);

    DBEntry e;
    dbkey_t k {2,2,3};
    e.set_key(k);

    c.insert(move(e));
    EQ(c.size(), 1);
    EQ(c.key_exist(k), true)
    dbkey_t bad_key {1,1,1};
    EQ(c.key_exist(bad_key), false);

    TEST_PASS

}

TEST(get_keys_ipc) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    EQ(c.size(), 0);

    size_t num_entries = 20;
    for (size_t i=0; i < num_entries; i++) {
        DBEntry e;
        dbkey_t k {1,1,boost::numeric_cast<vtx_t>(i)};
        e.set_key(k);

        c.insert(move(e));
    }

    EQ(c.size(), num_entries);
    
    auto keys = c.keys();

    EQ(keys.size(), num_entries);

    for (auto & key : keys) {
        EQ(c.key_exist(key), true);
    }

    TEST_PASS
}

TEST(get_keys_ipc_large) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    EQ(c.size(), 0);

    //was only working with 5481 entries or less, seg faulting with more, so test with at least that many
    size_t num_entries = 10000;
    for (size_t i=0; i < num_entries; i++) {
        DBEntry e;
        dbkey_t k {1,1,boost::numeric_cast<vtx_t>(i)};
        e.set_key(k);

        c.insert(move(e));
    }

    EQ(c.size(), num_entries);
    
    auto keys = c.keys();

    EQ(keys.size(), num_entries);

    for (auto & key : keys) {
        EQ(c.key_exist(key), true);
    }

    TEST_PASS
}

TEST(retrieve_all) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    // Create a lot of entries, all with long values
    size_t num_entries = 1000;
    map<dbkey_t, int> check_entry;
    for (chain_info_t i = 0; i < num_entries; i++) {
        DBEntry<> e;
        string val (1000+i, 'a');
        e.value() = val;
        dbkey_t key {i, 0,0};
        ++check_entry[key];
        e.set_key(key);
        c.insert(move(e));
    }

    auto entries = c.retrieve_all_entries();
    EQ(entries.size(), num_entries);

    for (auto & entry : entries) {
        auto key = entry.get_key();
        ++check_entry[key];

        uint64_t i = key.a;
        if (i > num_entries) TEST_FAIL

        EQ(key.b, 0);
        EQ(key.c, 0);

        EQ(entry.value().size(), 1000+i);
    }

    TEST_PASS

}

TEST(start_stop_quickly) {
    ZMQAddress addr {"127.0.0.1", 0};
    for (size_t i = 0; i < 25; i++) {
        //Just test to make sure no errors are thrown during this process
        ParDBThread<PandoMap> m {addr};
        PandoMapClient {addr, addr};
    }

    TEST_PASS
}

TEST(retrieve_if_exists) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    DBEntry<> e;
    e.value() = "test1";
    dbkey_t key {1,2,3};
    e.set_key(key);
    EQ(e.get_key(), key); 
    c.insert(move(e));

    auto [entry1, exists1] = c.retrieve_if_exists(key);
    EQ(exists1, true)
    EQ(entry1.value(), "test1");
    EQ(entry1.get_key(), key); 

    dbkey_t not_exist_key {2,3,4};
    auto [entry2, exists2] = c.retrieve_if_exists(not_exist_key);
    EQ(exists2, false)
    EQ(entry2.tags().size(), 0)
    EQ(entry2.get_key(), INITIAL_KEY)

    TEST_PASS
}

TEST(insert_multiple) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr};
    PandoMapClient c {map_addr, map_addr};

    absl::flat_hash_map<dbkey_t, DBEntry<>> entries;

    // Create a lot of entries, all with long values
    size_t num_entries = 1000;
    for (chain_info_t i = 0; i < num_entries; i++) {
        DBEntry<> e;
        string val (1000+i, 'a');
        e.value() = val;
        dbkey_t key {i, 0,0};
        e.set_key(key);
        entries[key] = move(e);
    }

    EQ(entries.size(), num_entries);
    c.insert_multiple(&entries);

    EQ(c.size(), num_entries);

    TEST_PASS
}

TEST(large_msg) {
    ZMQAddress map_addr {"127.0.0.1", ++g_idx};
    ParDBThread<PandoMap> m {map_addr, 16ull*(1ull<<30)};
    PandoMapClient c {map_addr, map_addr};

    string val;
    for (size_t j = 0; j < 1000; ++j) {
        val += "hello world";
    }

    size_t num_entries_to_add = 250000;

    for (size_t i = 0; i < num_entries_to_add; ++i) {
        DBEntry<> e;
        dbkey_t key {i, 0, 0};
        e.set_key(key);
        e.value() = val;
        e.add_tag("test");
        c.insert(move(e));
    }

    auto entries = c.retrieve_all_entries();

    EQ(entries.size(), num_entries_to_add);

    TEST_PASS

}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(insert_retrieve)
    RUN_TEST(insert_retrieve_size_ipc)
    RUN_TEST(key_exist_ipc)
    RUN_TEST(get_keys_ipc)
    RUN_TEST(get_keys_ipc_large)
    RUN_TEST(retrieve_all)
    //RUN_TEST(start_stop_quickly) //FIXME does this test even make sense? fails on ubuntu
    RUN_TEST(retrieve_if_exists)
    RUN_TEST(insert_multiple)
    RUN_TEST(large_msg)
    elga::ZMQChatterbox::Teardown();
TESTS_END
