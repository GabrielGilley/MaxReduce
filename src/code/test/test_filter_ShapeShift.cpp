#include <iostream>
#include <string>

#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(tag_added) {

    // Load the test data
    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ShapeShiftConvert");

    for (auto & [key, entry] : db.entries())
        EQ(entry.has_tag("ShapeShiftConvert:done"), false);

    db.process();

    for (auto & [key, entry] : db.entries()){
        EQ(entry.has_tag("ShapeShiftConvert:done"), true);
        
    }
    TEST_PASS
}

TEST(key_added){
    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ShapeShiftConvert");

    db.process();
    
    
    

    for (auto & [key, entry] : db.entries()){
        if(entry.get_key().b == 190472464494474227){
            auto k_copy = entry.get_key();
            EQ(k_copy.a, 0)
            EQ(k_copy.b, 190472464494474227)
            EQ(k_copy.c, 775508051078011794)
           }
         if(entry.get_key().b == 120318981819211655){
            auto k_copy = entry.get_key();
            EQ(k_copy.a, 0)
            EQ(k_copy.b, 120318981819211655)
            EQ(k_copy.c, 1086926560472837000)
           }
         if(entry.get_key().b == 245805704114159241){
            auto k_copy = entry.get_key();
            EQ(k_copy.a, 0)
            EQ(k_copy.b, 245805704114159241)
            EQ(k_copy.c, 39672448820194501)
           }
        
        if(entry.has_tag("ShapeShift_parsed")){
            EQ(is_random_key(key), false);
         
        }
    }
    
    TEST_PASS
}
TEST(test_size) {
    SeqDB db { data_dir+"/ShapeShiftData/ShapeShiftData.txt" };

    db.add_filter_dir(build_dir + "/filters");

    db.install_filter("ShapeShiftConvert");


    for (auto & [key, entry] : db.entries()) {
        EQ(entry.has_tag("ShapeShiftConvert:fail"), false);
    }
    EQ(db.size(), 5554);



    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(tag_added)
    RUN_TEST(key_added)  
    RUN_TEST(test_size)
    elga::ZMQChatterbox::Teardown();
TESTS_END
