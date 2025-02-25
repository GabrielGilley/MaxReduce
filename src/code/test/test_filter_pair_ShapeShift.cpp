#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(tag_added) {
   
    /* Test to check if a transaction that should be merged in the shapeshift data is
    * adds two transactions one from bitcoin and on from ethereum that are matched in the shapeshift data
    * These are close enough in time to get picked up by the round_tx_vals filter as a match
    */

    // Load the test data
    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };
    db.add_db_file(data_dir+"/exchange_rates/bitcoin_exchange_rate");
    db.add_db_file(data_dir+"/exchange_rates/ethereum_exchange_rate");
    
    //I think these should be merging but I am not seeing any merging when I run the test now though

    string btc_block = "{\
        \"tx\": [\
            {\
                \"txid\": \"f21cb1bd5fdb1d25625af67c61d95d58f0df363c0efa6dfdf222c7061bf305bd\",\
                \"hash\": \"f21cb1bd5fdb1d25625af67c61d95d58f0df363c0efa6dfdf222c7061bf305bd\",\
                \"vin\": [\
                    {\
                        \"txid\": \"1ff01d18d7a9b01a38b73410fba40e11e8ae9a11a26ba88f130027a0a2641b94\",\
                        \"vout\": 0\
                    }\
                ],\
                \"vout\": [\
                    {\
                        \"value\": 0.13921950,\
                        \"n\": 0\
                    }\
                ]\
            }\
        ],\
        \"time\": 1573662842\
    }";

    string eth_block = "{\
            \"hash\": \"0xfb857d9d9c10aab7f2987b775e00496e0c5c2fd062a57b05f5ac392dc5fd5802\",\
            \"timestamp\": \"0x5DCC306C\",\
            \"transactions\": [\
                {\
                    \"from\": \"0xd3273eba07248020bf98a8b560ec1576a612102f\",\
                    \"hash\": \"0xcf163b9cc6e6053f9b09a3e7ebc75642ba996b8e457d1b041cbc6dde21e68954\",\
                    \"to\": \"0xd546dc96295dd3c189c4521dddcc356ebe1837e0\",\
                    \"value\": \"0x5A01911330D69400\"\
                }\
            ]\
        }";

    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "block").value() = btc_block;
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ETH", "block").value() = eth_block;
        db.add_entry(move(e));
    }
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_vals");
    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_block_to_txtime");
    db.install_filter("ETH_tx_vals");
    db.install_filter("ShapeShiftConvert");
    db.install_filter("exchange_rates");
    db.install_filter("findShapeShiftPairs");
    db.process();

    // Enforce findShapeShiftPairs to be run before round_tx_vals to ensure it
    // will still work out of order

    db.install_filter("round_tx_vals");
    
    for (auto & [key, entry] : db.entries()){
        EQ(entry.has_tag("findShapeShiftPairs:done"), false);
    }
    db.process();

    size_t num_found = 0;
    for (auto & [key, entry] : db.entries()){
        if(entry.has_tag("MERGED")){
            EQ(entry.has_tag("ShapeShift_transaction_found"), true);
            num_found++;
        }
    }

    EQ(num_found, 9);
    TEST_PASS
}
TEST(tag_not_added){

    /* Test to check if a transaction that shouldn't be merged in the shapeshift data is
    * adds two transactions one from bitcoin and on from ethereum that are matched 
    * in the shapeshift data but with timestamps farther apart then what round_tx_vals would catch
    */

    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };
    db.add_db_file(data_dir+"/exchange_rates/bitcoin_exchange_rate");
    db.add_db_file(data_dir+"/exchange_rates/ethereum_exchange_rate");
        
    string btc_block = "{\
        \"tx\": [\
            {\
                \"txid\": \"bc2365e84e734b26bcfc6f9cc32d4a59a22efeb70eebb63d924b626f15966f75\",\
                \"hash\": \"bc2365e84e734b26bcfc6f9cc32d4a59a22efeb70eebb63d924b626f15966f75\",\
                \"vin\": [\
                    {\
                        \"txid\": \"feef5df492e48b83b0f3a92359db67a8a0c6a64453e65ebbb7d26ee840429df7\",\
                        \"vout\": 0\
                    }\
                ],\
                \"vout\": [\
                    {\
                        \"value\": 35.885,\
                        \"n\": 0\
                    }\
                ]\
            }\
        ],\
        \"time\": 1581970380\
    }";

    string eth_block = "{\
            \"hash\": \"0x97d62a66efe3ab681dea2ed837de909b31fc42b57ce8641cda1de6ea8bb2c089\",\
            \"timestamp\": \"0x5E4AAD93\",\
            \"transactions\": [\
                {\
                    \"from\": \"0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413\",\
                    \"hash\": \"0x22e064475c5825703ceca2d741c4f8df00b87c3e12a957a79e697b009baa7ee0\",\
                    \"to\": \"0x5a3853c2551f4ca82053f25e5d03738b8451b741\",\
                    \"value\": \"487139430000000000\"\
                }\
            ]\
        }";

    {
        DBEntry<> e;
        e.clear().add_tag("BTC", "block").value() = btc_block;
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.clear().add_tag("ETH", "block").value() = eth_block;
        db.add_entry(move(e));
    }
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("BTC_block_to_tx");
    db.install_filter("BTC_block_to_txtime");
    db.install_filter("BTC_tx_vals");
    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_block_to_txtime");
    db.install_filter("ETH_tx_vals");
    db.install_filter("ShapeShiftConvert");
    db.install_filter("exchange_rates");
    db.install_filter("round_tx_vals");
    db.install_filter("findShapeShiftPairs");

    for (auto & [key, entry] : db.entries()){
        EQ(entry.has_tag("findShapeShiftPairs:done"), false);
    }
    db.process();

    for (auto & [key, entry] : db.entries()){
        std::string s = "";
        if(entry.has_tag("MERGED")){
            EQ(entry.has_tag("ShapeShift_transaction_found"), false);
            TEST_FAIL
        }
    }
    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(tag_not_added)  
    elga::ZMQChatterbox::Teardown();
TESTS_END
