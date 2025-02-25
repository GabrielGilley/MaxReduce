#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

#include <chrono>

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(simple_run) {
    // Load the test data
    SeqDB db { data_dir+"/simple_bitcoin.txt" };

    db.add_filter_dir(build_dir+"/filters");

    db.install_filter("BTC_block_number_to_tx");

    db.process();

    dbkey_t expected_key {917504, 447438596448131869, 0};
    string expected_value = "100000";

    size_t num_entries = 0;
    bool entry_found = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("BLOCK_NUMBER_LOOKUP")) {
            ++num_entries;

            if (key == expected_key) {
                entry_found = true;
                EQ(entry.value(), expected_value);
            }
        }

    }

    EQ(num_entries, 5);
    EQ(entry_found, true);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(simple_run)
    elga::ZMQChatterbox::Teardown();
TESTS_END
