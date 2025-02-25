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

    db.install_filter("ETH_block_to_tx");
    db.install_filter("ETH_tx_edges");
    db.install_filter("ETH_tx_dataset_creation");

    db.process();

    size_t addr_to_input_count = 0;
    size_t addr_to_output_count = 0;
    size_t input_count = 0;
    size_t output_count = 0;
    size_t tx_to_input_count = 0;
    size_t tx_to_output_count = 0;
    size_t addr_count = 0;
    for (auto &[key, entry] : db.entries()) {
        if (entry.has_tag("address_to_tx_inputs.csv")) {
            ++addr_to_input_count;
        } else if (entry.has_tag("address_to_tx_outputs.csv")) {
            ++addr_to_output_count;
        } else if (entry.has_tag("tx_inputs.csv")) {
            ++input_count;
        } else if (entry.has_tag("tx_outputs.csv")) {
            ++output_count;
        } else if (entry.has_tag("tx_to_inputs.csv")) {
            ++tx_to_input_count;
        } else if (entry.has_tag("tx_to_outputs.csv")) {
            ++tx_to_output_count;
        }
        else if (entry.has_tag("address.csv")) {
            ++addr_count;
        }
    }

    const size_t tx_count = 23;

    EQ(addr_to_input_count, tx_count);
    EQ(addr_to_output_count, tx_count);
    EQ(input_count, tx_count);
    EQ(output_count, tx_count);
    EQ(tx_to_input_count, tx_count);
    EQ(tx_to_output_count, tx_count);
    EQ(addr_count, 32);

    TEST_PASS
}



TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(entries_added)
    elga::ZMQChatterbox::Teardown();
TESTS_END
