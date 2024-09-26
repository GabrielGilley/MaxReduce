#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(entries_added) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };

    db.add_filter_dir(build_dir+"/filters");

    db.install_filter("ETH_block_dataset_creation");

    db.process();

    size_t block_csv_count = 0;
    size_t block_to_bc_count = 0;
    size_t block_to_tx_count = 0;
    bool found_tx_entry = false; // test one of these to make sure
    string block1_hash = "0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c3";
    string block2_hash = "0xa86e991fd8477152dd3088b521a2a52b142064741d7d76b665d12cfbd270fafc";
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("blocks.csv")) {
            ++block_csv_count;
            bool match_block_1 = block1_hash == entry.value().substr(0, block1_hash.size());
            bool match_block_2 = block2_hash == entry.value().substr(0, block2_hash.size());
            EQ(match_block_1 || match_block_2, true)
        }
        else if (entry.has_tag("block_to_blockchain.csv")) {
            ++block_to_bc_count;
            bool match_block_1 = entry.value() == block1_hash +",ETH";
            bool match_block_2 = entry.value() == block2_hash +",ETH";
            EQ(match_block_1 || match_block_2, true)
        }
        else if (entry.has_tag("tx_to_block.csv")) {
            ++block_to_tx_count;
            if (entry.value() == "0x398afa0125dc65bad4ce57bf7e3e3d11d5d34f546128d108d29be583971af63e," + block2_hash)
                found_tx_entry = true;
        }
    }

    EQ(block_csv_count, 2);
    EQ(block_to_bc_count, 2);
    EQ(block_to_tx_count, 23);
    EQ(found_tx_entry, true);

    TEST_PASS
}



TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(entries_added)
    elga::ZMQChatterbox::Teardown();
TESTS_END
