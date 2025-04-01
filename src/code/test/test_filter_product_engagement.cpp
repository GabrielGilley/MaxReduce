#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

#include <chrono>

using namespace std;
using namespace pando;
using namespace std::chrono;

TEST(simple_run) {
    SeqDB db;

    // table1
    // USER_ID, PRODUCT_ID, ENGAGEMENT_SCORE
    {
        DBEntry<> e;
        e.value() = "1,1,62";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "1,2,49";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "1,3,5";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "2,1,90";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "2,2,12";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "2,3,50";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "3,1,7";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "3,2,42";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "3,3,75";
        e.add_tag("product_engagement_table1");
        db.add_entry(move(e));
    }

    // table2
    // PRODUCT_ID, PRODUCT_SPECS
    {
        DBEntry<> e;
        e.value() = "1,100";
        e.add_tag("product_engagement_table2");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "2,43";
        e.add_tag("product_engagement_table2");
        db.add_entry(move(e));
    }
    {
        DBEntry<> e;
        e.value() = "3,86";
        e.add_tag("product_engagement_table2");
        db.add_entry(move(e));
    }

    db.add_filter_dir(build_dir+"/filters");
    db.install_filter("product_engagement_set_keys");
    db.install_filter("product_engagement");
    db.process();


    size_t num_joined_found = 0;
    bool entry_found = false;
    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("product_engagement_join")) {
            ++num_joined_found;

            if (entry.value() == "1,1,62,100") {
                entry_found = true;
            }
            cout << entry << "\n------" << endl;
        }

    }

    EQ(num_joined_found, 4);
    EQ(entry_found, true);



    TEST_PASS
}


TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(simple_run)
    elga::ZMQChatterbox::Teardown();
TESTS_END
