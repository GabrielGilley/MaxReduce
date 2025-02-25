#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(etl_tokens) {
    SeqDB db { data_dir+"/ethereum/etl_tokens.txt" };
    EQ(db.entries().size(), 87);

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("etl_tokens");
    db.process();
    EQ(db.entries().size(), 174);

    size_t count = 0;
    for (auto& [key, entry] : db.entries()) {
        if (entry.has_tag("ethereum_etl_token_address_parsed")) {
            count++;
        }
        if (entry.has_tag("etl_tokens:fail"))
            TEST_FAIL
    }

    EQ(count, 87);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(etl_tokens)
    elga::ZMQChatterbox::Teardown();
TESTS_END
