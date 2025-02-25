#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

#include <chrono>
#include <boost/algorithm/string.hpp>

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(tag_added) {
    // Load the test data
    SeqDB db { data_dir+"/simple_bitcoin.txt" };
    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_vout_addrs");

    db.process();

    // make sure we find one specific entry as a test
    string expected_value = "1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn";
    dbkey_t expected_key {851968, 1152680873570800642, 0};

    size_t num_addrs = 0;
    bool entry_found = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("VOUT_ADDRESS")) {
            ++num_addrs;
        }
        if (key == expected_key) {
            EQ(entry.value(), expected_value);
            entry_found = true;
        }
    }

    EQ(entry_found, true);
    EQ(num_addrs, 5);

    TEST_PASS
}

TEST(multisig) {
    SeqDB db;

    DBEntry e;
    e.add_tag("BTC", "tx");
    e.value() = " { \"tx\": { \"txid\": \"7ad2ff6c3bad807927101bca1c4cc341231028463748d913137f5fd99dc23e2b\", \"hash\": \"7ad2ff6c3bad807927101bca1c4cc341231028463748d913137f5fd99dc23e2b\", \"vin\": [ { \"txid\": \"4ba1f3d1ae93c2de5acc3c924f051b2a31449b922c1614c4ab1304928a773555\", \"vout\": 2, \"scriptSig\": { \"asm\": \"3045022100b9ff6543a6e431c9846f81a2e113a80f98e26a6c651448115175cbcddf00b353022033c036a0d1d56c839eea858b5cf918355509379cfe1c0be10bfa4b665492619a[ALL] 02aece047d2125fd863c5131978979ca3e4a7668b2331a5caa10819fd6b27643ff\", \"hex\": \"483045022100b9ff6543a6e431c9846f81a2e113a80f98e26a6c651448115175cbcddf00b353022033c036a0d1d56c839eea858b5cf918355509379cfe1c0be10bfa4b665492619a012102aece047d2125fd863c5131978979ca3e4a7668b2331a5caa10819fd6b27643ff\" } } ], \"vout\": [ { \"value\": 0.0000125, \"n\": 0, \"scriptPubKey\": { \"addresses\": [ \"1NjMEnqV47fHXUujB47BPLuYjR4Wa8oHV1\" ] } }, { \"value\": 0.0000125, \"n\": 1, \"scriptPubKey\": { \"addresses\": [ \"1FkkVBpsfUUcQYFxX63gR5UUfZLqpc3gMs\", \"13adMPJMszMKuzvwFJ8Tp12aHeW3jB5xUX\", \"1LW1DFzppD1TS7Gp68wzaAK8F2QD7TjRM3\" ] } }, { \"value\": 0.0125343, \"n\": 2, \"scriptPubKey\": { \"addresses\": [ \"1LW1DFzppD1TS7Gp68wzaAK8F2QD7TjRM3\" ] } } ] } }";
    // NOTE: Not setting key here, shouldn't be needed, but may in the future

    db.add_entry(move(e));

    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("BTC_vout_addrs");
    db.process();

    size_t num_addrs = 0;
    size_t num_addr_entries = 0;

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("VOUT_ADDRESS")) {
            ++num_addr_entries;
            // there may be multiple addresses per entry, so count how many lines are in each entry
            vector<boost::iterator_range<std::string::iterator>> results;
            boost::algorithm::find_all(results, entry.value(), "\n");
            num_addrs += results.size() + 1;
        }
    }

    EQ(num_addr_entries, 3);
    EQ(num_addrs, 5);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(multisig)
    elga::ZMQChatterbox::Teardown();
TESTS_END
