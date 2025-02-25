#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(ingest) {
    // Load the test data
    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };

    EQ(db.size(), 5554);

    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(ingest)
    elga::ZMQChatterbox::Teardown();
TESTS_END
