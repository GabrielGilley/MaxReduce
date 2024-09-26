#include "seq_db.hpp"
#include "alg_db.hpp"
#include "dbkey.h"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(get_stats) {
    AlgDB db { data_dir+"/simple_bitcoin.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_in_edges");
    db.install_filter("BTC_tx_out_edges");
    db.install_filter("BTC_utxo_edges");
    db.install_filter("BTC_utxo_stats");
    db.process();

    bool dec_found = false;
    bool feb_found = false;
    //Count the number of month buckets that it finds, should only be one for
    //this small test
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("utxo_stats")) {
            if (entry.has_tag("month=1291161600")) {
                EQ(dec_found, false);
                dec_found = true;
                EQ(entry.value(), "month=1291161600\nnum_utxos=6");
            }
            if (entry.has_tag("month=1296518400")) {
                EQ(feb_found, false);
                feb_found = true;
                EQ(entry.value(), "month=1296518400\nnum_utxos=1");
            }
        }
    }

    EQ(dec_found, true);
    EQ(feb_found, true);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(get_stats)
    elga::ZMQChatterbox::Teardown();
TESTS_END
