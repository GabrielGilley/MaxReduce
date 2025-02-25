#include "seq_db.hpp"
#include "dbkey.h"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(build_utxo_edges) {
    SeqDB db { data_dir+"/zcash_blocks/zcash_blocks_small.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ZEC_block_to_tx");
    db.install_filter("ZEC_block_to_txtime");
    db.install_filter("ZEC_tx_in_edges");
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    db.install_filter("ZEC_utxo_edges");
    db.process();

    size_t ts1_count = 0;
    size_t ts2_count = 0;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("utxo_edges") && entry.has_tag("ZEC")) {
            EQ(key.a, 4295098368);
            if (key.b == 1475280000) {
                //EQ(key.c, 3779042324779104703); FIXME validate c key
                ts1_count++;
            }
            else
                ts2_count++;
        }
    }

    EQ(ts1_count, 419);
    EQ(ts2_count, 0);

    //Make sure running it again doesn't change anything
    size_t num_entries = db.entries().size();
    db.process();
    EQ(num_entries, db.entries().size());

    TEST_PASS
}

TEST(manual_utxo_edges) {
    SeqDB db;

    dbkey_t entry_key {0, 0, 0};
    {DBEntry<> e; e.add_tag("ZEC", "from=A", "to=UTXO"); e.set_key(entry_key); db.add_entry(e); }
    {DBEntry<> e; e.add_tag("ZEC", "from=B", "to=UTXO"); e.set_key(entry_key); db.add_entry(e); }
    {DBEntry<> e; e.add_tag("ZEC", "from=C", "to=UTXO"); e.set_key(entry_key); db.add_entry(e); }
    {DBEntry<> e; e.add_tag("ZEC", "A", "txtime"); e.value() = "1629853063"; db.add_entry(e); }
    {DBEntry<> e; e.add_tag("ZEC", "B", "txtime"); e.value() = "1595379463"; db.add_entry(e); }
    {DBEntry<> e; e.add_tag("ZEC", "C", "txtime"); e.value() = "1360893463"; db.add_entry(e); }

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_utxo_edges");
    db.process();

    size_t ts1_count = 0;
    size_t ts2_count = 0;
    size_t ts3_count = 0;

    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("utxo_edges") && entry.has_tag("ZEC")) {
            EQ(key.a, 131072);

            if (key.b == 1593586800) {
                //EQ(key.c, 3779042324779104703); FIXME validate c key
                ts1_count++;
            } else if (key.b == 1627801200) {
                //EQ(key.c, 3779042324779104703); FIXME validate c key
                ts2_count++;
            } else if (key.b == 1359705600) {
                //EQ(key.c, 3779042324779104703); FIXME validate c key
                ts3_count++;
            }
        }
    }

    EQ(ts1_count, 1);
    EQ(ts2_count, 1);
    EQ(ts3_count, 1);

    TEST_PASS
}

TEST(time_conversion) {
    SeqDB db;

    chain_info_t edge_info = pack_chain_info(ZEC_KEY, TX_OUT_EDGE_KEY, UTXO_KEY);
    {DBEntry<> e; e.add_tag("ZEC", "from=A", "to=UTXO"); e.set_key({edge_info, 10, 0}); db.add_entry(move(e)); }
    {DBEntry<> e; e.add_tag("ZEC", "from=B", "to=UTXO"); e.set_key({edge_info, 11, 0}); db.add_entry(move(e)); }
    {DBEntry<> e; e.add_tag("ZEC", "from=C", "to=UTXO"); e.set_key({edge_info, 12, 0}); db.add_entry(move(e)); }
    {DBEntry<> e; e.add_tag("ZEC", "from=D", "to=UTXO"); e.set_key({edge_info, 13, 0}); db.add_entry(move(e)); }
    chain_info_t txtime_key = pack_chain_info(ZEC_KEY, TXTIME_KEY, 0);
    {DBEntry<> e; e.add_tag("ZEC", "A", "txtime"); e.value() = "1477641360"; e.set_key({txtime_key, 10, 0}); db.add_entry(move(e)); } //October 28, 2016
    {DBEntry<> e; e.add_tag("ZEC", "B", "txtime"); e.value() = "1478097340"; e.set_key({txtime_key, 11, 0}); db.add_entry(move(e)); } //November 2, 2016
    {DBEntry<> e; e.add_tag("ZEC", "C", "txtime"); e.value() = "1481355912"; e.set_key({txtime_key, 12, 0}); db.add_entry(move(e)); } //December 10, 2016
    {DBEntry<> e; e.add_tag("ZEC", "D", "txtime"); e.value() = "1483254000"; e.set_key({txtime_key, 13, 0}); db.add_entry(move(e)); } //January 5, 2017

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ZEC_utxo_edges");
    db.process();
    size_t oct_count = 0;
    size_t nov_count = 0;
    size_t dec_count = 0;
    size_t jan_count = 0;


    for (auto &[key,entry] : db.entries()) {
        if (entry.has_tag("utxo_edges") && entry.has_tag("ZEC")) {
            if (key.b == 1475280000) {
                oct_count++;
            } else if (key.b == 1477958400) {
                nov_count++;
            } else if (key.b == 1480550400) {
                dec_count++;
            } else if (key.b == 1483228800) {
                jan_count++;
            }
        }
    }

    EQ(oct_count, 1);
    EQ(nov_count, 1);
    EQ(dec_count, 1);
    EQ(jan_count, 1);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(build_utxo_edges)
    //RUN_TEST(manual_utxo_edges)
    RUN_TEST(time_conversion)
    elga::ZMQChatterbox::Teardown();
TESTS_END
