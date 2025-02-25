#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;


TEST(decode) {
    SeqDB db { data_dir+"/simple_ethereum.txt" };
    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_smart_contract_decode");
    db.process();
    for (auto & [key, entry] : db.entries()) {
        //if (entry.has_tag("ETH_smart_contract_decode:fail")) {
        if (entry.has_tag("decoded_smart_contract")) {
            //cout << entry << "\n--------------" << endl;
        }
    }

    TEST_PASS
}

TEST(delete_me) {
    //One test has to exist, so delete this when other tests are added

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    //RUN_TEST(decode)
    RUN_TEST(delete_me)
    elga::ZMQChatterbox::Teardown();
TESTS_END
