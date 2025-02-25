#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(entries_added) {
    SeqDB db { data_dir+"/simple_bitcoin.txt" };

    db.add_filter_dir(build_dir+"/filters");

    db.install_filter("BTC_block_dataset_creation");

    db.process();

    size_t block_csv_count = 0;
    size_t block_to_bc_count = 0;
    size_t block_to_tx_count = 0;
    size_t addr_count = 0;
    bool found_tx_entry = false; // test one of these to make sure
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("blocks.csv")) {
            ++block_csv_count;
            string block1_hash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506";
            string block2_hash = "0000000000004bb16ecb3bf59323e258cf7da88e7bf9981dd943d6f25d3132df";
            bool match_block_1 = block1_hash == entry.value().substr(0, block1_hash.size());
            bool match_block_2 = block2_hash == entry.value().substr(0, block2_hash.size());
            EQ(match_block_1 || match_block_2, true)
        }
        else if (entry.has_tag("block_to_blockchain.csv")) {
            ++block_to_bc_count;
            bool match_block_1 = entry.value() == "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506,BTC";
            bool match_block_2 = entry.value() == "0000000000004bb16ecb3bf59323e258cf7da88e7bf9981dd943d6f25d3132df,BTC";
            EQ(match_block_1 || match_block_2, true)
        }
        else if (entry.has_tag("tx_to_block.csv")) {
            ++block_to_tx_count;
            if (entry.value() == "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87,000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506")
                found_tx_entry = true;
        }
        else if (entry.has_tag("address.csv")) {
            ++addr_count;
        }
    }

    EQ(block_csv_count, 2);
    EQ(block_to_bc_count, 2);
    EQ(block_to_tx_count, 5);
    EQ(addr_count, 5);
    EQ(found_tx_entry, true);

    TEST_PASS
}

TEST(no_duplicate_addrs) {
    SeqDB db { data_dir+"/bitcoin_for_subgraph_search.txt" };
    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("BTC_block_dataset_creation");
    db.process();

    size_t num_addrs = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("address.csv"))
            ++num_addrs;
    }

    EQ(num_addrs, 16095);
    
    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(entries_added)
    RUN_TEST(no_duplicate_addrs)
    elga::ZMQChatterbox::Teardown();
TESTS_END
