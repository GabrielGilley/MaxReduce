#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(tether_manual) {
    SeqDB db { data_dir+"/exchange_rates/tether_exchange_rate"};
    {
        DBEntry<> e;
        e.clear().add_tag("OMNI", "tx").value() = "{\"amount\": \"119.58000000\",\"block\": 813385,\"blockhash\": \"00000000000000000030daf500f4656bd84bc68a333683c29633aa53e64fa7c\", \"blocktime\": 1698008799, \"confirmations\": 2491, \"divisible\": true, \"fee\": \"0.00005739\",\"flags\": null, \"ismine\": false, \"positioninblock\": 85, \"propertyid\": 31, \"propertyname\": \"TetherUS\", \"referenceaddress\": \"3G8sspvb7tzrPAG3KQp3UqKshBRQSFKdN5\", \"sendingaddress\": \"31xrhv44PfSRbGrU83iCNDqeyRdGP79eXn\",\"txid\": \"a56f1611dafcab86d45b957c69a5d7a03d48d3b3c1129751f772bfd0b019d578\", \"type\": \"Simple Send\", \"type_int\": 0, \"valid\": true, \"version\": 0}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(BTC_KEY, OMNI_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("find_tether");
    db.process();
    int found_tether = 0;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("tx") && entry.has_tag("USDT")) {
            found_tether ++;
            }
    }
    EQ(found_tether, 1); 
    TEST_PASS
}

TEST(not_tether_manual) {
    SeqDB db;
    {
        DBEntry<> e;
        e.clear().add_tag("OMNI", "tx").value() = "{\"amount\": \"1007\", \"block\": 815743,  \"blockhash\": \"0000000000000000000314290ad7f62b14516667959137aeeb512a9810a4e988\",  \"blocktime\": 1699386353, \"confirmations\": 139, \"divisible\": false, \"fee\": \"0.00025789\", \"flags\": null, \"ismine\": false, \"positioninblock\": 1451, \"propertyid\": 3, \"propertyname\": \"MaidSafeCoin\", \"referenceaddress\": \"12t2XPWFyptbgSDM4fvxwLp9rzR7hAbHsM\", \"sendingaddress\": \"1DUb2YYbQA1jjaNYzVXLZ7ZioEhLXtbUru\", \"txid\": \"b662543e452aec049907f14b2bd8f2fb0e54c2f66453f3d6e7c7d87b71ec6b91\", \"type\": \"Simple Send\", \"type_int\": 0, \"valid\": true, \"version\": 0}";
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        chain_info_t ci = pack_chain_info(BTC_KEY, OMNI_KEY, 0);
        vtx_t b_key = 10; //A
        dbkey_t key = {ci, b_key, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    // Load and run the filter
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("find_tether");
    db.process();
    bool found_tether = false;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("USDT")) {
            found_tether = true;
            }
        }
    
    EQ(found_tether, false);
    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tether_manual)
    RUN_TEST(not_tether_manual)
    elga::ZMQChatterbox::Teardown();
TESTS_END
