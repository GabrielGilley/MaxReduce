#include "test.hpp"

#include "big_space.hpp"

#include <string>

using namespace std;
using namespace pando;

TEST(bigspace) {

    BigSpace sp;

    char* a1 = (char*)sp.allocate(12);
    char* a2 = (char*)sp.allocate(2);

    EQ(a2-a1, 12);

    TEST_PASS
}

TEST(oom) {
    BigSpace sp { 6 };

    char* a1 = (char*)sp.allocate(1);
    char* a2 = (char*)sp.allocate(1);
    EQ(a2-a1, 1);

    try {
        char* a3 = (char*)sp.allocate(5);
        (void)a3;
        TEST_FAIL
    } catch(const bad_alloc &e) { }

    TEST_PASS
}

TEST(alloc_raw) {
    auto sp = std::make_shared<BigSpace>((2+3)*sizeof(int));
    BigSpaceAllocator<int> bsa1{sp};
    int* int_sp1 = bsa1.allocate(2);
    BigSpaceAllocator<int> bsa2{sp};
    int* int_sp2 = bsa2.allocate(3);

    int_sp1[0] = 0;
    int_sp1[1] = 1;

    int_sp2[0] = 0;
    int_sp2[1] = 1;
    int_sp2[2] = 2;

    // fixme finish writing this...

    TEST_PASS
}

TEST(alloc_str) {
    auto sp = std::make_shared<BigSpace>(1ull<<10);
    char* first_byte = (char*)sp->allocate(1);
    BigSpaceAllocator<char> alloc{sp};
    basic_string<char, char_traits<char>, BigSpaceAllocator<char>> str {alloc};
    str.assign("0000000000000000"); // NOTE on gcc 11.2, short string optimization kicks in with string lengths 15 or less
    NOPRINT_EQ(first_byte, str.c_str()-1);

    TEST_PASS
}

TEST(str_ops) {
    using Str = basic_string<char, char_traits<char>, BigSpaceAllocator<char>>;

    auto sp = std::make_shared<BigSpace>(1ull<<10);
    BigSpaceAllocator<char> alloc{sp};
    Str str {alloc};
    str.assign("0000000000000000asdf");

    auto next_str = str;

    NOPRINT_EQ(str.c_str(), next_str.c_str()-31);   // NOTE on gcc 11.2, str will be allocated 32 chars

    const char* next_str_ptr = next_str.c_str();

    Str mv_str = move(next_str);

    NOPRINT_EQ(next_str_ptr, mv_str.c_str());

    TEST_PASS
}

TEST(copy_alloc) {
    auto sp = std::make_shared<BigSpace>(6);
    BigSpaceAllocator<char> a1{sp};
    auto a2 = a1;

    char* s1 = a1.allocate(1);
    char* s2 = a2.allocate(1);

    EQ(s2-s1, 1);

    TEST_PASS
}

TEST(move_alloc) {
    auto sp = std::make_shared<BigSpace>(6);
    BigSpaceAllocator<char> a1{sp};
    char* s1 = a1.allocate(1);

    auto a2 = std::move(a1);

    char* s2 = a2.allocate(1);

    EQ(s2-s1, 1);

    TEST_PASS
}

TEST(str_plus_equals) {
    using Str = basic_string<char, char_traits<char>, BigSpaceAllocator<char>>;
    
    auto sp = std::make_shared<BigSpace>(1ull<<10);
    BigSpaceAllocator<char> alloc{sp};
    Str str {alloc};
    str.assign("0000000000000000asdf");

    const char* str_ptr = str.c_str();

    str += "testing";

    NOPRINT_EQ(str.c_str(), str_ptr); 

    str += "1111111111111111";

    NOPRINT_EQ(str.c_str(), str_ptr+31); 

    TEST_PASS
}

TEST(str_plus_equals_two) {
    using Str = basic_string<char, char_traits<char>, BigSpaceAllocator<char>>;
    
    auto sp = std::make_shared<BigSpace>(1ull<<10);
    BigSpaceAllocator<char> alloc{sp};
    Str str1 {alloc};
    Str str2 {alloc};

    str1 = "0000000000000000ABC";
    str2 = "1111111111111111111";

    str1 += "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    str2 += "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

    const char* p1 = str1.c_str();
    Str str3 = move(str1);
    const char* p2 = str3.c_str();
    NOPRINT_EQ(p1, p2);

    cout << str2 << endl << str3 << endl;

    TEST_PASS
}

TEST(str_move) {
    using Str = basic_string<char, char_traits<char>, BigSpaceAllocator<char>>;
    
    auto sp = std::make_shared<BigSpace>(1ull<<10);
    BigSpaceAllocator<char> alloc{sp};
    Str str {alloc};
    str.assign("0000000000000000asdf");

    Str str2 {alloc};
    str2 = move(str);
    
    TEST_PASS
}

TESTS_BEGIN
    RUN_TEST(bigspace)
    RUN_TEST(oom)
    RUN_TEST(alloc_raw)
    RUN_TEST(alloc_str)
    RUN_TEST(str_ops)
    RUN_TEST(copy_alloc)
    RUN_TEST(move_alloc)
    RUN_TEST(str_plus_equals)
    RUN_TEST(str_plus_equals_two)
    RUN_TEST(str_move)
TESTS_END
