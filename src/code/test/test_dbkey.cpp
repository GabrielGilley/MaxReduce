#include "test.hpp"

#include "dbkey.h"

using namespace std;

TEST(init_key) {
    //the default value should be 0 here, but could be defined differently for a DBEntry
    dbkey_t k {};
    EQ(k.a, 0);
    EQ(k.b, 0);
    EQ(k.c, 0);

    TEST_PASS
}

TEST(dbkey_equal_equal) {
    dbkey_t a {0, 1, 2};
    dbkey_t b {0, 1, 2};
    dbkey_t c {1, 1, 2};
    dbkey_t d {0, 3, 2};
    dbkey_t e {0, 1, 3};

    EQ(a == b, true);
    EQ(a == c, false);
    EQ(a == d, false);
    EQ(a == e, false);
    EQ(c == d, false);

    EQ(a != b, false);
    EQ(a != c, true);
    EQ(a != d, true);
    EQ(a != e, true);
    EQ(c != d, true);


    TEST_PASS
}

TEST(dbkey_lt_gt) {
    dbkey_t a {0, 1, 2};
    dbkey_t b {0, 1, 2};
    dbkey_t c {1, 1, 2};
    dbkey_t d {0, 3, 2};
    dbkey_t e {0, 1, 3};

    EQ((a < b), false);
    EQ((a < c), true);
    EQ((a < d), true);
    EQ((a < e), true);
    EQ((e < a), false);

    EQ((a > b), false);
    EQ((a > c), false);
    EQ((a > d), false);
    EQ((a > e), false);
    EQ((e > a), true);

    TEST_PASS
}

TEST(pack_chain_info) {
    uint32_t a = BTC_KEY;
    uint16_t b = TX_IN_EDGE_KEY;
    uint16_t c = UTXO_KEY;

    EQ(BTC_KEY, 0);
    EQ(TX_IN_EDGE_KEY, 0);
    EQ(UTXO_KEY, 1);

    vtx_t result = pack_chain_info(a,b,c);

    EQ(result, 1);

    a = ZEC_KEY;
    EQ(a, 1)

    result = pack_chain_info(a,b,c);

    EQ(result, 4294967297);

    TEST_PASS
}

TEST(unpack_chain_info) {
    //should become 0000000000000000000000000000010001000001000001000100001000001100
    vtx_t input = 18270667276;

    uint32_t a;
    uint16_t b;
    uint16_t c;

    unpack_chain_info(input, &a, &b, &c);

    EQ(a, 4)
    EQ(b, 16644)
    EQ(c, 16908)

    TEST_PASS
}

TEST(use_key) {
    chain_info_t encoded = pack_chain_info(ZEC_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
    vtx_t b = 10;
    vtx_t c = 12;
    dbkey_t key {encoded, b, c};

    EQ(key.a, 4295032832);
    EQ(key.b, 10);
    EQ(key.c, 12);

    uint32_t decoded;
    uint16_t b_res, c_res;

    unpack_chain_info(key.a, &decoded, &b_res, &c_res);

    EQ(decoded, ZEC_KEY)
    EQ(b_res, TX_OUT_EDGE_KEY);
    EQ(c_res, NOT_UTXO_KEY);

    TEST_PASS
}

TEST(pack_unpack_rand) {
    uint32_t a = (1llu << 31) | 1;
    uint16_t b = 2;
    uint16_t c = 3;

    chain_info_t result = pack_chain_info(a,b,c);

    EQ(result, ((((((1llu<<31) | 1)<<16) | b) << 16) | c));

    uint32_t na;
    uint16_t nb;
    uint16_t nc;

    unpack_chain_info(result, &na, &nb, &nc);

    EQ(a, na)
    EQ(b, nb)
    EQ(c, nc)

    TEST_PASS
}

TESTS_BEGIN
    RUN_TEST(dbkey_equal_equal)
    RUN_TEST(init_key)
    RUN_TEST(pack_chain_info)
    RUN_TEST(unpack_chain_info)
    RUN_TEST(use_key)
    RUN_TEST(dbkey_lt_gt)
    RUN_TEST(pack_unpack_rand)
TESTS_END
