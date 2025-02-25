#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;
using namespace std::chrono;

// NOTE: This file would make sense to combine with
// test_filter_ETH_tx_dataset_creation.cpp. However, it seems that python
// filters get messed up when used by multiple test cases. If we learn that
// isn't true and fix the problem, these files could be merged together

TEST(duplicate_addrs) {
    SeqDB db;

    {
        DBEntry<> e; e.value() = "{\"hash\": \"0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c3\", \"number\": \"0x2dc6c0\",\"timestamp\": \"0x587b4a9b\", \"transactions\": [{\"blockHash\": \"0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c3\", \"blockNumber\": \"0x2dc6c0\", \"from\": \"0x987654\",\"hash\": \"0x7c03009d52ec50069adb4311d5225fe2249ce5afb3a9951af258f0f680efb19a\", \"to\": \"0x123456\", \"value\": \"0x44b1eec6162f0000\"} ] }";
        e.add_tag("ETH", "block");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e; e.value() = "{\"hash\": \"0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c4\", \"number\": \"0x2dc6c1\",\"timestamp\": \"0x587b4b9b\", \"transactions\": [{\"blockHash\": \"0xee396a86beaade9d6057b72a92b7bf5b40be4997745b437857469557b562a7c4\", \"blockNumber\": \"0x2dc6c1\", \"from\": \"0x987654\",\"hash\": \"0x7c03009d52ec50069adb4311d5225fe2249ce5afb3a9951af258f0f680efb19b\", \"to\": \"0x123457\", \"value\": \"0x44b1eec6162f0001\"} ] }";
        e.add_tag("ETH", "block");
        db.add_entry(move(e));
    }
    db.add_filter_dir(build_dir+"/filters");

    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_tx_edges");
    db.install_filter("ETH_tx_dataset_creation");

    db.process();


    size_t addr_count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("address.csv")) {
            ++addr_count;
        }
    }

    EQ(addr_count, 3);

    TEST_PASS
}



TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(duplicate_addrs)
    elga::ZMQChatterbox::Teardown();
TESTS_END
