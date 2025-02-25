#include <iostream>
#include <string>
#include <boost/algorithm/string.hpp>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(etl_token_tx) {
    SeqDB db { data_dir+"/ethereum/etl_token_transfers.txt" };
    EQ(db.entries().size(), 283);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("etl_token_transfers");
    db.process();
    // Note that there are 122 duplicate transactions,
    // therefore we should only expect 283+(283-122) entries
    // after processing (i.e. only 161 new entries)
    EQ(db.entries().size(), 444);

    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("ethereum_etl_token_transfer_parsed"))
            count++;
    }

    EQ(count, 161);

    TEST_PASS
}
TEST(etl_token_tx_merge) {
    SeqDB db;

    string block_1 = "token_address:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2\ntransaction_hash:0x41e49f1e174d991a9c4e690a2c11b2069cd3ead9ae458946214fba06e6c1c7a0\nvalue:198250000000000000\nblock:14400000\nfrom_address:0xdef1c0ded9bec7f1a1670819833240f027b25eff\nto_address:0xc0bf97bffa94a50502265c579a3b7086d081664b";
    string block_2 = "token_address:0x990f341946a3fdb507ae7e52d17851b87168017c\ntransaction_hash:0x41e49f1e174d991a9c4e690a2c11b2069cd3ead9ae458946214fba06e6c1c7a0\nvalue:3271250784691066588\nblock:14400000\nfrom_address:0xc0bf97bffa94a50502265c579a3b7086d081664b\nto_address:0x74de5d4fcbf63e00296fd95d33236b9794016631";
    string block_3 = "token_address:0x990f341946a3fdb507ae7e52d17851b87168017c\ntransaction_hash:0x41e49f1e174d991a9c4e690a2c11b2069cd3ead9ae458946214fba06e6c1c7a0\nvalue:3271250784691066588\nblock:14400000\nfrom_address:0x74de5d4fcbf63e00296fd95d33236b9794016631\nto_address:0x9db7205741d8caf463bce4836911b86e09a7be93";
    {
        DBEntry<> e;
        e.clear().add_tag("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "ethereum_etl_token_transfer").value() = block_1;
        db.add_entry(move(e));
    }

    // ENTRY 2
    {
        DBEntry<> e;
        e.clear().add_tag("0x990f341946a3fdb507ae7e52d17851b87168017c", "ethereum_etl_token_transfer").value() = block_2;
        db.add_entry(move(e));
    }

    // ENTRY 3
    {
        DBEntry<> e;
        e.clear().add_tag("0x990f341946a3fdb507ae7e52d17851b87168017c", "ethereum_etl_token_transfer").value() = block_3;
        db.add_entry(move(e));
    }

    // Sanity check
    EQ(db.entries().size(), 3);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("etl_token_transfers");
    db.process();

    // Note that there are 3 duplicate transactions,
    // which result in 1 unique duplicate transactions
    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("MERGED")){
            size_t num_merged = 0;

            vector<string> values;
            boost::split(values, entry.value(), boost::is_any_of("\n"));
            for (std::string line: values){
                if (boost::starts_with(line, "token_address:")) {
                    num_merged++;
                }
            }
            // Ensure that there are multiple lines that have token addresses
            EQ(num_merged, 3);

            // Increase count of merged entries
            count++;
        }
    }

    EQ(count, 1);

    TEST_PASS
}

TEST(etl_token_tx_merge_data) {

    SeqDB db { data_dir+"/ethereum/etl_token_transfers.txt" };
    EQ(db.entries().size(), 283);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("etl_token_transfers");
    db.process();
    // Note that there are 122 duplicate transactions,
    // therefore we should only expect 283+(283-122) entries
    // after processing (i.e. only 161 new entries)
    EQ(db.entries().size(), 444);

    // Note that there are 3 duplicate transactions,
    // which result in 1 unique duplicate transactions
    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("MERGED")){
            size_t num_merged = 0;

            vector<string> values;
            boost::split(values, entry.value(), boost::is_any_of("\n"));
            for (std::string line: values){
                if (boost::starts_with(line, "token_address:")) {
                    num_merged++;
                }
            }
            // Ensure that there are multiple lines that have token addresses
            if (num_merged <= 0){
                TEST_FAIL;
            }

            // Increase count of merged entries
            count++;
        }
    }

    EQ(count, 72);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(etl_token_tx)
    RUN_TEST(etl_token_tx_merge)
    RUN_TEST(etl_token_tx_merge_data)
    elga::ZMQChatterbox::Teardown();
TESTS_END
