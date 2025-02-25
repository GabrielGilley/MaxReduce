#include "seq_db.hpp"
#include "alg_db.hpp"
#include "dbkey.h"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(get_stats) {
    AlgDB db { data_dir+"/zcash_blocks/zcash_blocks_small.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");
    db.install_filter("ZEC_block_to_txtime");
    db.install_filter("ZEC_tx_in_edges");
    db.install_filter("ZEC_tx_out_edges");
    db.install_filter("ZEC_utxo_edges");
    db.install_filter("ZEC_utxo_stats");
    db.process();

    bool oct_found = false;
    bool other_found = false;
    //Count the number of month buckets that it finds, should only be one for
    //this small test
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("utxo_stats")) {
            if (entry.has_tag("month=1475280000")) {
                EQ(oct_found, false);
                oct_found = true;
                EQ(entry.value(), "month=1475280000\nnum_utxos=419");
            }
            else {
                EQ(other_found, false);
                other_found = true;
            }
        }
    }

    EQ(oct_found, true);
    EQ(other_found, false);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(get_stats)
    elga::ZMQChatterbox::Teardown();
TESTS_END
