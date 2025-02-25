#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

#define SIMPLE_ETH_NUM_TX 23
#define SIMPLE_ETH_NUM_BLOCKS 2

TEST(create_edges) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };
    EQ(db.size(), SIMPLE_ETH_NUM_BLOCKS);

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_tx_edges");
    db.process();

    // subtract one because there's one duplicate src/dst pair
    size_t expected_db_size = SIMPLE_ETH_NUM_BLOCKS + \
                              SIMPLE_ETH_NUM_TX + \
                              SIMPLE_ETH_NUM_TX \
                              - 1 ;
    EQ(db.size(), expected_db_size);

    // make sure we find one specific entry as a test
    string expected_from_tag = "from=0x6c7f03ddfdd8a37ca267c88630a4fee958591de0";
    string expected_to_tag = "to=0xf91a6e0a861e8b3c53c29f2772871588afd49ce6";
    dbkey_t expected_key {8590000128, 1908684504423818, 4382270570389992};

    // There were some problems with tags being correct in python filters, so
    // make sure we test for that explicitly
    size_t eth_entries = 0;
    size_t out_edge_entries = 0;
    size_t edge_entries = 0;

    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("ETH"))
            ++eth_entries;
        if (entry.has_tag("tx-out-edge"))
            ++out_edge_entries;
        if (entry.has_tag("tx-edge"))
            ++edge_entries;
        if (entry.get_key() == expected_key) {
            EQ(entry.has_tag(expected_from_tag), true);
            EQ(entry.has_tag(expected_to_tag), true);
            EQ(entry.has_tag("tx-edge"), true);
            EQ(entry.has_tag("tx-out-edge"), true);
            EQ(entry.value(), "0x3a41dc97f36705a,0x5a4e7ae6b4c9fff3a797131fd083be2274e20c51a9d929730a31374c270a797d")
        }
    }

    EQ(eth_entries, expected_db_size);
    EQ(out_edge_entries, SIMPLE_ETH_NUM_TX - 1);
    EQ(edge_entries, SIMPLE_ETH_NUM_TX - 1);


    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(create_edges)
    elga::ZMQChatterbox::Teardown();
TESTS_END
