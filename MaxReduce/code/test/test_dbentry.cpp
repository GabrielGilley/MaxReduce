#include "test.hpp"

#include <sstream>
#include <vector>
#include <string>

#include "dbentry.hpp"

using namespace std;
using namespace pando;

TEST(add_tags) {
    DBEntry<> db;

    db.add_tag("first");
    db.add_tag("second");
    db.add_tag("third");

    EQ(db.tag_size(), 3);

    EQ(db.has_tag("no"), false);
    EQ(db.has_tag("second"), true);
    EQ(db.has_tag("third"), true);
    EQ(db.has_tag("no"), false);

    db.add_tag("fourth").add_tag("fifth");
    EQ(db.tag_size(), 5);
    EQ(db.has_tag("fourth"), true);
    EQ(db.has_tag("fifth"), true);

    db.add_tag("6", "7", "8");
    EQ(db.has_tag("6"), true);
    EQ(db.has_tag("7"), true);
    EQ(db.has_tag("8"), true);

    TEST_PASS
}

TEST(remove_tag) {
    DBEntry<> e;

    e.add_tag("first", "second", "third");
    EQ(e.tag_size(), 3);

    e.remove_tag("first", "fourth");

    EQ(e.tag_size(), 2);
    EQ(e.has_tag("first"), false);
    EQ(e.has_tag("second"), true);
    EQ(e.has_tag("third"), true);

    //Verify that the c tags got updated
    bool second_found = false;
    bool third_found = false;
    const char* const* tags = e.c_tags();
    while ((*tags)[0] != '\0') {
        if (string(*tags) == "first")
            TEST_FAIL
        else if (string(*tags) == "second") {
            if (second_found)
                TEST_FAIL
            second_found = true;
        } else if (string(*tags) == "third") {
            if (third_found)
                TEST_FAIL
            third_found = true;
        } else {
            TEST_FAIL
        }
        ++tags;
    }

    EQ(second_found, true);
    EQ(third_found, true);


    TEST_PASS
}

TEST(check_value) {
    DBEntry<> db;

    EQ(db.value(), "");
    db.add_to_value("X");
    EQ(db.value(), "X\n");
    const string& v {db.value()};
    EQ(v, "X\n");
    db.add_to_value("another string");
    EQ(db.value(), "X\nanother string\n");
    EQ(v, "X\nanother string\n");

    string& edit_v {db.value()};
    EQ(edit_v, "X\nanother string\n");
    edit_v.assign("--");
    EQ(db.value(), "--");
    EQ(v, "--");

    db.add_to_value("a").add_to_value("b");
    EQ(v, "--a\nb\n");

    TEST_PASS
}

TEST(clear) {
    DBEntry<> db;

    EQ(db.tag_size(), 0);
    EQ(db.value(), "");

    db.add_tag("t1");
    EQ(db.tag_size(), 1);
    EQ(db.value(), "");

    db.add_to_value("Y");
    EQ(db.tag_size(), 1);
    EQ(db.value(), "Y\n");

    auto& r = db.clear();
    EQ(r.tag_size(), 0);
    EQ(r.value(), "");
    EQ(db.tag_size(), 0);
    EQ(db.value(), "");

    TEST_PASS
}

TEST(c_tags) {
    DBEntry<> db;

    db.add_tag("first");
    db.add_tag("second");
    db.add_tag("third");

    EQ(db.tag_size(), 3);

    auto t = db.c_tags();
    auto p = t;

    vector<string> tags;

    while (*p[0] != '\0') {
        tags.emplace_back(*p++);
    }
    EQ(tags.size(), 3);

    bool t1 = false;
    bool t2 = false;
    bool t3 = false;

    for (size_t idx = 0; idx < 3; ++idx) {
        string tag { t[idx] };
        if (tag == "first") {
            if (t1) TEST_FAIL
            t1 = true;
        } else if (tag == "second") {
            if (t2) TEST_FAIL
            t2 = true;
        } else if (tag == "third") {
            if (t3) TEST_FAIL
            t3 = true;
        } else
            TEST_FAIL
    }
    EQ(t1 && t2 && t3, true);

    TEST_PASS
}

TEST(tags) {
    DBEntry<> db;

    db.add_tag("first");
    db.add_tag("second");
    db.add_tag("third");

    EQ(db.tag_size(), 3);

    auto tags = db.tags();

    EQ(tags.size(), 3);

    bool t1 = false;
    bool t2 = false;
    bool t3 = false;

    for (auto &tag : tags) {
        if (tag == "first") {
            if (t1) TEST_FAIL
            t1 = true;
        } else if (tag == "second") {
            if (t2) TEST_FAIL
            t2 = true;
        } else if (tag == "third") {
            if (t3) TEST_FAIL
            t3 = true;
        } else
            TEST_FAIL
    }
    EQ(t1 && t2 && t3, true);

    TEST_PASS
}

TEST(clear_f) {
    DBEntry<> db;

    NOPRINT_NOTEQ(db.c_tags(), nullptr);

    db.add_tag("Tag");

    auto t = db.c_tags();
    NOPRINT_NOTEQ(t, nullptr);
    EQ(t[0][0], 'T');
    EQ(t[1][0], '\0');

    db.clear();

    t = db.c_tags();
    NOPRINT_NOTEQ(t, nullptr);
    EQ(t[0][0], '\0');

    db.add_tag("Tag");

    t = db.c_tags();
    NOPRINT_NOTEQ(t, nullptr);
    EQ(t[0][0], 'T');
    EQ(t[1][0], '\0');

    EQ(db.valid_c_tags(), true);
    db.clear(true);
    EQ(db.valid_c_tags(), false);
    try {
        db.c_tags();
        TEST_FAIL
    } catch(...) {}

    TEST_PASS
}

TEST(init_key) {
    DBEntry<> e;
    e.clear(true);
    auto k = e.get_key();
    EQ(k, INITIAL_KEY);
    EQ(k.a, INIT_CH_IN);
    EQ(k.b, -1);
    EQ(k.c, -1);

    TEST_PASS
}

TEST(key) {
    DBEntry<> e;
    e.set_key({3, 5, 10});
    auto k_copy = e.get_key();
    EQ(k_copy.a, 3);
    EQ(k_copy.b, 5);
    EQ(k_copy.c, 10);
    auto& k = e.get_key();
    EQ(k.a, 3);
    EQ(k.b, 5);
    EQ(k.c, 10);

    dbkey_t k2 = {17, 15, 20};
    e.set_key(k2);
    EQ(k.a, 17);
    EQ(k.b, 15);
    EQ(k.c, 20);
    EQ(k_copy.a, 3);
    EQ(k_copy.b, 5);
    EQ(k_copy.c, 10);

    TEST_PASS
}

TEST(copy_constructor) {
    DBEntry<> db1;

    db1.add_tag("t1");
    db1.add_tag("t2");
    string& db1_val = db1.value();
    db1_val.assign("some content");
    db1.set_key({1,0,0});

    EQ(db1.value(), "some content");
    EQ(db1.tag_size(), 2);
    EQ(db1.random_key(), false);
    EQ(db1.get_key().a, 1);
    EQ(db1.get_key().b, 0);
    EQ(db1.get_key().c, 0);

    DBEntry<> db2 { db1 };

    EQ(db2.value(), "some content");
    EQ(db2.tag_size(), 2);
    EQ(db2.get_key(), INITIAL_KEY);
    NOTEQ(db2.get_key().a, 1);
    NOTEQ(db2.get_key().b, 0);
    NOTEQ(db2.get_key().c, 0);
    auto c2 = db2.get_key().c;
    db1_val.assign("new");
    db1.add_tag("t3");
    db1.set_key({15,20, 22});
    EQ(db1.tag_size(), 3);
    EQ(db2.tag_size(), 2);
    EQ(db1.value(), "new");
    EQ(db2.value(), "some content");
    EQ(db1.get_key().a, 15);
    EQ(db1.get_key().b, 20);
    EQ(db1.get_key().c, 22);
    NOTEQ(db2.get_key().a, 1);
    NOTEQ(db2.get_key().b, 0);
    EQ(db2.get_key().c, c2);

    TEST_PASS
}

TEST(copy_assignment) {
    DBEntry<> db1;

    db1.add_tag("t1");
    db1.add_tag("t2");
    string& db1_val = db1.value();
    db1_val.assign("some content");
    db1.set_key({1,0,0});

    DBEntry<> db2;
    db2.add_tag("x");

    EQ(db1.value(), "some content");
    EQ(db1.tag_size(), 2);
    EQ(db2.get_key().a, INIT_CH_IN);
    EQ(db2.get_key().b, -1);
    EQ(db2.get_key().c, -1);
    auto c2 = db2.get_key().c;

    db2 = db1;
    EQ(db2.get_key(), INITIAL_KEY);
    c2 = db2.get_key().c;
    EQ(db2.value(), "some content");
    EQ(db2.tag_size(), 2);
    db1_val.assign("new");
    db1.add_tag("t3");
    db1.set_key({50,40,30});
    EQ(db1.tag_size(), 3);
    EQ(db2.tag_size(), 2);
    EQ(db1.value(), "new");
    EQ(db2.value(), "some content");
    NOTEQ(db2.get_key().a, 1);
    NOTEQ(db2.get_key().b, 0);
    EQ(db2.get_key().c, c2);
    EQ(db1.get_key().a, 50);
    EQ(db1.get_key().b, 40);
    EQ(db1.get_key().c, 30);

    TEST_PASS
}

TEST(move_constructor) {
    DBEntry<> db1;

    db1.add_tag("t1");
    db1.add_tag("t2");
    string& db1_val = db1.value();
    db1_val.assign("some content");
    db1.set_key({1,0,0});

    EQ(db1.value(), "some content");
    EQ(db1.tag_size(), 2);
    EQ(db1.get_key().a, 1);
    EQ(db1.get_key().b, 0);
    EQ(db1.get_key().c, 0);

    DBEntry<> db2 {move(db1)};
    EQ(db2.value(), "some content");
    EQ(db2.tag_size(), 2);
    EQ(db1_val, "")
    try {
        db1.tag_size();
        TEST_FAIL
    } catch(...) {}
    EQ(db1.get_key().a, INIT_CH_IN);
    EQ(db1.get_key().b, -1);
    EQ(db1.get_key().c, -1);
    EQ(db2.get_key().a, 1);
    EQ(db2.get_key().b, 0);
    EQ(db2.get_key().c, 0);
    EQ(db2.tag_size(), 2);
    EQ(db2.value(), "some content");

    TEST_PASS
}

TEST(move_assignment) {
    DBEntry<> db1;

    db1.add_tag("t1");
    db1.add_tag("t2");
    string& db1_val = db1.value();
    db1_val.assign("some content");
    db1.set_key({1,0,0});

    EQ(db1.value(), "some content");
    EQ(db1.tag_size(), 2);
    EQ(db1.get_key().a, 1);
    EQ(db1.get_key().b, 0);
    EQ(db1.get_key().c, 0);

    DBEntry<> db2;
    db2.add_tag("x");
    NOPRINT_NOTEQ(db2.c_tags(), nullptr);

    db2 = move(db1);

    EQ(db2.value(), "some content");
    EQ(db2.tag_size(), 2);
    EQ(db1_val, "")
    try {
        db1.tag_size();
        TEST_FAIL
    } catch(...) {}
    EQ(db1.get_key().a, INIT_CH_IN);
    EQ(db1.get_key().b, -1);
    EQ(db1.get_key().c, -1);
    EQ(db2.get_key().a, 1);
    EQ(db2.get_key().b, 0);
    EQ(db2.get_key().c, 0);
    EQ(db2.tag_size(), 2);
    EQ(db2.value(), "some content");

    TEST_PASS
}

TEST(access) {
    DBEntry<> db;

    auto access = db.access();

    EQ(access->key, db.get_key());

    delete access;

    TEST_PASS
}

TEST(access_tagval) {
    DBEntry<> db;

    db.add_tag("k1", "k2");
    EQ(db.tag_size(), 2);

    auto access = db.access();

    EQ(access->tags, db.c_tags());
    EQ(access->value, db.value().c_str());

    delete access;

    TEST_PASS
}

TEST(build_from_c) {
    const char* tags[] = {"t1", "t2", ""};
    const char* value = "test val";

    DBEntry<> e {tags, value};

    EQ(e.tag_size(), 2);
    EQ(e.value(), "test val");
    EQ(e.has_tag("t1"), true);
    EQ(e.has_tag("t2"), true);

    TEST_PASS
}

TEST(empty_init) {
    DBEntry<> e;

    NOPRINT_NOTEQ(e.c_tags(), nullptr);

    TEST_PASS
}

TEST(fail_on_invalid) {
    DBEntry<> start_entry;
    start_entry.add_tag("t1");
    start_entry.add_tag("t2");
    start_entry.add_to_value("content");
    EQ(start_entry.tag_size(), 2);
    EQ(start_entry.valid_c_tags(), true);

    auto moved_entry = move(start_entry);

    try { start_entry.add_tag("a"); TEST_FAIL; } catch(...) {}
    try { start_entry.add_to_value("a"); TEST_FAIL; } catch(...) {}
    try { start_entry.tag_size(); TEST_FAIL; } catch(...) {}
    try { start_entry.has_tag("asdf"); TEST_FAIL; } catch(...) {}
    try { start_entry.has_tag("t1"); TEST_FAIL; } catch(...) {}
    try { start_entry.c_tags(); TEST_FAIL; } catch(...) {}
    EQ(start_entry.valid_c_tags(), false);
    try { start_entry.c_tags(); TEST_FAIL; } catch(...) {}
    try {
        const DBEntry<>& c_entry = start_entry;
        [[maybe_unused]] const string& v = c_entry.value();
        TEST_FAIL;
    } catch(...) {}
    try { [[maybe_unused]] string& v = start_entry.value(); TEST_FAIL; } catch(...) {}
    try { start_entry.access(); TEST_FAIL; } catch(...) {}

    TEST_PASS
}

TEST(const_value) {
    DBEntry<> e;
    e.add_to_value("some value");
    string &v1 = e.value();
    EQ(v1, "some value\n");
    v1.assign("next");
    EQ(e.value(), "next");
    v1 = "further";
    EQ(e.value(), "further");

    const DBEntry<>& c = e;

    EQ(e.value(), "further");
    EQ(c.value(), "further");
    e.add_to_value("x");
    EQ(e.value(), "furtherx\n");
    EQ(c.value(), "furtherx\n");

    TEST_PASS
}

TEST(print_entry) {
    DBEntry<> e;
    e.add_tag("test1").add_tag("cout_test");
    EQ(e.has_tag("test1"), true);
    EQ(e.has_tag("cout_test"), true);
    e.value() = "test_value";

    stringstream ss;
    ss << e;
    string s = ss.str();

    if (s != "KEY:\n(random)\nTAGS:\ncout_test\ntest1\nVALUE:\ntest_value" &&
        s != "KEY:\n(random)\nTAGS:\ntest1\ncout_test\nVALUE:\ntest_value")
        TEST_FAIL

    TEST_PASS
}

TEST(print_entry_key) {
    DBEntry<> e;
    e.add_tag("test1").add_tag("cout_test");
    EQ(e.has_tag("test1"), true);
    EQ(e.has_tag("cout_test"), true);
    e.value() = "test_value";
    e.set_key({3,4,5});

    stringstream ss;
    ss << e;
    string s = ss.str();

    if (s != "KEY:\n3:4:5\nTAGS:\ncout_test\ntest1\nVALUE:\ntest_value" &&
        s != "KEY:\n3:4:5\nTAGS:\ntest1\ncout_test\nVALUE:\ntest_value")
        TEST_FAIL

    TEST_PASS
}

TEST(build_with_key) {
    const char* const tags[] = {"test1", "test2", ""};
    const char* value = "test_value";
    dbkey_t key {1,2,3};
    DBEntry<> e {tags, value, key};

    EQ(e.get_key().a, 1);
    EQ(e.get_key().b, 2);
    EQ(e.get_key().c, 3);

    TEST_PASS
}

TEST(not_equal) {
    dbkey_t e1_key = {10,10,10};
    dbkey_t e4_key = {11,10,10};
    DBEntry<> e1; e1.value() = "foo\nbar"; e1.add_tag("test1", "test2"); e1.set_key(e1_key);
    DBEntry<> e2; e2.value() = "foo\nbar"; e2.add_tag("test1", "test2"); e2.set_key(e1_key);
    DBEntry<> e3; e3.value() = "foo\nbar"; e3.add_tag("test1", "test3"); e3.set_key(e1_key);
    DBEntry<> e4; e4.value() = "foo\nbar"; e4.add_tag("test1", "test2"); e4.set_key(e4_key);
    DBEntry<> e5; e5.value() = "oo\nbar"; e5.add_tag("test1", "test2"); e5.set_key(e1_key);

    //Everything the same
    EQ(false, e1 != e2);

    //Different tags
    EQ(true, e1 != e3);

    //Different key
    EQ(true, e1 != e4);

    //Different value
    EQ(true, e1 != e5);

    TEST_PASS
}

TEST(serialize) {
    dbkey_t e1_key = {10,10,10};
    DBEntry<> e1; e1.value() = "foo\nbar"; e1.add_tag("test1", "test2"); e1.set_key(e1_key);

    char* e1s = new char[e1.serialize_size()];
    char* e1s_ptr = e1s;
    EQ((unsigned long)e1s_ptr, (unsigned long)e1s);

    e1.serialize(e1s_ptr);
    NOTEQ((unsigned long)e1s_ptr, (unsigned long)e1s);

    const char* e1s_des = e1s;
    DBEntry<> e2 {e1s_des};
    EQ((unsigned long)e1s_ptr, (unsigned long)e1s_des);

    EQ(false, e1 != e2);

    // Test that e1s moved up
    delete [] e1s;
    
    TEST_PASS
}

TESTS_BEGIN
    RUN_TEST(add_tags)
    RUN_TEST(remove_tag)
    RUN_TEST(check_value)
    RUN_TEST(clear)
    RUN_TEST(c_tags)
    RUN_TEST(tags)
    RUN_TEST(clear_f)
    RUN_TEST(init_key)
    RUN_TEST(key)
    RUN_TEST(copy_constructor)
    RUN_TEST(copy_assignment)
    RUN_TEST(move_constructor)
    RUN_TEST(move_assignment)
    RUN_TEST(access)
    RUN_TEST(access_tagval)
    RUN_TEST(build_from_c)
    RUN_TEST(empty_init)
    RUN_TEST(fail_on_invalid)
    RUN_TEST(const_value)
    RUN_TEST(print_entry)
    RUN_TEST(print_entry_key)
    RUN_TEST(build_with_key)
    RUN_TEST(not_equal)
    RUN_TEST(serialize)
TESTS_END
