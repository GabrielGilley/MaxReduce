#include <iostream>
#include <string>
#include "test.hpp"
#include "address.hpp"

using namespace std;
using namespace elga;


TEST(add_to_addr) {
    ZMQAddress a1 {"127.0.0.1", 0};
    ZMQAddress a2 = a1 + 1;
    ZMQAddress a3 = a2 + 3;

    EQ(a1.get_addr_str(), "127.0.0.1,0")
    EQ(a2.get_addr_str(), "127.0.0.1,1")
    EQ(a3.get_addr_str(), "127.0.0.1,4")

    TEST_PASS
}

TEST(sub_from_addr) {
    ZMQAddress a1 {"127.0.0.1", 4};
    ZMQAddress a2 = a1 - 1;
    ZMQAddress a3 = a2 - 3;

    try {
        ZMQAddress a4 = a3 - 1;
        TEST_FAIL;
    } catch (...) { }

    EQ(a1.get_addr_str(), "127.0.0.1,4")
    EQ(a2.get_addr_str(), "127.0.0.1,3")
    EQ(a3.get_addr_str(), "127.0.0.1,0")

    TEST_PASS
}



TESTS_BEGIN
    RUN_TEST(add_to_addr)
    RUN_TEST(sub_from_addr)
TESTS_END
