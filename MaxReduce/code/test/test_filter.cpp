#include <string>

#include "test.hpp"
#include "filter.hpp"

using namespace std;
using namespace pando;

TEST(build_filter) {
    try {
        Filter f { build_dir+"/test/libtestlib_simple.so" };
        EQ(0,0);

        // Check the name
        EQ(string { f.name() }, "simple");

        //Check to make sure it has a "should_run" function that will
        //return false
        const DBAccess* acc = nullptr;
        EQ(f.should_run(acc), false);

    } catch(...) {
        TEST_FAIL
    }

    TEST_PASS
}

TEST(build_filter_fail) {
    try {
        Filter f { build_dir+"/test/non-existent.so" };
        TEST_FAIL
    } catch(...) { }

    try {
        Filter f { build_dir+"/test/libtestlib_non_filter.so" };
        TEST_FAIL
    } catch(...) { }

    TEST_PASS
}

TEST(move_filter) {
    /* NOTE: will be invalid when function exists */

    FilterInterface i {
        filter_name: "TEST",
        filter_type: SINGLE_ENTRY,
        should_run: nullptr,
        init: nullptr,
        destroy: nullptr,
        run: nullptr
    };

    // Build a Filter
    Filter a {&i};

    Filter b = move(a);

    // Check a, b appropriately
    EQ(a.valid(), false);
    EQ(b.valid(), true);

    Filter c {&i};

    EQ(b.valid(), true);
    EQ(c.valid(), true);

    c = move(b);

    EQ(b.valid(), false);
    EQ(c.valid(), true);

    Filter d {};
    d = move(c);
    EQ(c.valid(), false);
    EQ(d.valid(), true);

    TEST_PASS
}

static int g_TEST_should_run_filter_pass;
bool TEST_should_run_filter([[maybe_unused]] const DBAccess* acc) {
    g_TEST_should_run_filter_pass = 10;
    return true;
}
TEST(should_run) {
    /* NOTE: will be invalid when function exists */

    FilterInterface i {
        filter_name: "TEST",
        filter_type: SINGLE_ENTRY,
        should_run: &TEST_should_run_filter,
        init: nullptr,
        destroy: nullptr,
        run: nullptr
    };

    // Build a Filter
    Filter a {&i};

    g_TEST_should_run_filter_pass = 1;
    bool ret = a.should_run(nullptr);
    EQ(ret, true);
    EQ(g_TEST_should_run_filter_pass, 10);

    TEST_PASS
}

static int g_TEST_run_filter_filter_pass;
void TEST_run_filter_filter([[maybe_unused]] void* access) {
    g_TEST_run_filter_filter_pass = 10;
}

TEST(run_filter) {
    /* NOTE: will be invalid when function exists */

    FilterInterface i {
        filter_name: "TEST",
        filter_type: SINGLE_ENTRY,
        should_run: nullptr,
        init: nullptr,
        destroy: nullptr,
        run: &TEST_run_filter_filter
    };

    // Build a Filter
    Filter a {&i};

    g_TEST_run_filter_filter_pass = 1;
    a.run(nullptr);
    EQ(g_TEST_run_filter_filter_pass, 10);
    
    TEST_PASS
}

TEST(check_type) {
    FilterInterface i {
        filter_name: "TEST",
        filter_type: SINGLE_ENTRY,
        should_run: nullptr,
        init: nullptr,
        destroy: nullptr,
        run: nullptr
    };
    EQ(i.filter_type, SINGLE_ENTRY);
    i.filter_type = GROUP_ENTRIES;
    EQ(i.filter_type, GROUP_ENTRIES);
    TEST_PASS
}

TEST(init_fail) {
    FilterInterface i {
        filter_name: "TEST",
        filter_type: SINGLE_ENTRY,
        should_run: nullptr,
        init: nullptr,
        destroy: nullptr,
        run: nullptr
    };
    Filter a {&i};
    try {
        a.init(nullptr);
        TEST_FAIL;
    } catch(...) { }
    try {
        a.destroy(nullptr);
        TEST_FAIL;
    } catch(...) { }
    TEST_PASS
}

TESTS_BEGIN
    RUN_TEST(build_filter)
    RUN_TEST(build_filter_fail)
    RUN_TEST(move_filter)
    RUN_TEST(should_run)
    RUN_TEST(run_filter)
    RUN_TEST(check_type)
    RUN_TEST(init_fail)
TESTS_END
