#include "test.hpp"

#include "seq_db.hpp"
#include "dbentry.hpp"
#include "filter.hpp"

using namespace std;
using namespace pando;

TEST(load_filter) {
    try {
        Filter f { build_dir+"/filters/libfind.so" };
        EQ(0,0);

        // Check the name
        EQ(string { f.name() }, "find");

        //Check to make sure it has a "should_run" function that will
        //return false
        const DBAccess* acc = nullptr;
        auto should_run_res = f.should_run(acc);
        EQ(should_run_res, false);

    } catch(...) {
        TEST_FAIL
    }

    TEST_PASS
}

TEST(should_run) {
    Filter f { build_dir+"/filters/libfind.so" };
    // Create a DB entry to evaluate on
    DBEntry<> e;
    e.add_tag("tag1");
    e.add_tag("tag2");
    e.add_tag("tag3");

    auto acc = e.access();

    auto should_run_res = f.should_run(acc);
    EQ(should_run_res, true);

    delete acc;

    TEST_PASS
}

TEST(should_not_run) {
    Filter f { build_dir+"/filters/libfind.so" };

    {
        // Create a DB entry to evaluate on
        DBEntry<> e;
        e.add_tag("tag1");
        e.add_tag("find:done");
        e.add_tag("tag3");

        auto acc = e.access();

        auto should_run_res = f.should_run(acc);
        EQ(should_run_res, false);

        delete acc;
    }
    {
        // Create a DB entry to evaluate on
        DBEntry<> e;
        e.add_tag("tag1");
        e.add_tag("find:fail");
        e.add_tag("tag3");

        auto acc = e.access();

        auto should_run_res = f.should_run(acc);
        EQ(should_run_res, false);

        delete acc;
    }

    TEST_PASS
}

TEST(run) {
    SeqDB db;

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("find");

    {
        DBEntry<> e;
        e.value().assign("somet");
        dbkey_t key = {99, 0, 1};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.add_to_value("there is something to search for in here...");
        dbkey_t key = {5, 0, 0};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.add_to_value("there is nothing to search for in here");
        dbkey_t key = {5, 0, 1};
        e.set_key(key);
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.add_to_value("but sometimes it's good to search anyway");
        dbkey_t key = {5, 0, 2};
        e.set_key(key);
        db.add_entry(move(e));
    }

    EQ(db.size(), 4);

    db.process();

    EQ(db.size(), 6);
    
    TEST_PASS
}

TEST(run_fail) {
    SeqDB db;

    db.add_filter_dir(build_dir + "/filters");
    db.install_filter("find");

    {
        DBEntry<> e;
        e.add_to_value("no query was given");
        dbkey_t key = {5, 0, 2};
        e.set_key(key);
        db.add_entry(move(e));
    }

    EQ(db.size(), 1);

    db.process();

    EQ(db.size(), 1);
    auto entry = db.entries().begin()->second;
    auto tags = entry.tags();
    EQ(tags.size(), 1);
    NOPRINT_EQ(*(tags.begin()), "find:fail");
    
    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(load_filter)
    RUN_TEST(should_run)
    RUN_TEST(should_not_run)
    RUN_TEST(run)
    RUN_TEST(run_fail)
    elga::ZMQChatterbox::Teardown();
TESTS_END
