#include "helpers.hpp"
#include <time.h>
#include <sstream>

using namespace std;
using namespace pando;

void fail_(const DBAccess *access, const char* filter_name, const char* filter_fail_tag) {
    cerr << "[ERROR] unable to parse JSON in entry for filter: "<< filter_name << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool should_run_btc_based_utxo_edges(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    auto tags = access->tags;
    bool found_name = false;
    bool found_utxo = false;

    // Run on 'tx-out-edge's
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_name = true;
        // FIXME: make filter that creates UTXO add a new tag which causes
        // a 'recompute', separate from running the first time
        if (string(*tags) == "to=UTXO")
            found_utxo = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_name && found_utxo)
        return true;
    return false;
}

void btc_based_utxo_edges(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);

    // Get the timestamp from the source
    // We should have a tag: from=<txid>
    auto tags = access->tags;
    string txid;
    while ((*tags)[0] != '\0') {
        string tag {*tags};
        if (tag.substr(0,5) == "from=")
            txid.assign(tag.substr(5,tag.size()));
        ++tags;
    }

    // Find the value of the entry with
    // txtime
    // <txid>
    // FIXME test this by mixing ZEC and BTC in the same DB

    chain_info_t chain_info = pack_chain_info(crypto_id, TXTIME_KEY, 0);

    vtx_t src_key;
    stringstream ss;
    ss << txid.substr(0,15);
    ss >> hex >> src_key;

    dbkey_t search_key {chain_info, src_key, 0};

    char* ts_ret = nullptr;
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &ts_ret);
    if (ts_ret == nullptr) {
        // Process again later
        return;
    }

    // Get the timestamp
    string ts { ts_ret };
    free(ts_ret);

    // Get the vout from the key
    uint64_t txfrom = access->key.b;
    uint64_t vout_num = access->key.c;

    // Find the month in months-since-1970
    time_t raw_ts = stoi(ts);
    time_t month_ts;
    {
        struct tm* ts_tm = gmtime(&raw_ts);
        // Remove the month
        ts_tm->tm_mday = 1;
        ts_tm->tm_sec = 0;
        ts_tm->tm_hour = 0;
        ts_tm->tm_min = 0;
        month_ts = mktime(ts_tm) - timezone; //timezone is a constant provided by the time std library
    }

    // Create the edge that can be processed by a group filter
    // <0>
    dbkey_t new_key;
    //Use 0 because we don't need anything for that portion
    new_key.a = pack_chain_info(crypto_id, UTXO_EDGE, 0);
    new_key.b = month_ts;
    new_key.c = (0xfffff000 & txfrom) | vout_num;

    // Create new DB entries for each transaction
    const char* new_tags[] = {crypto_tag, "utxo_edges", ""};
    const char* new_value = "";

    access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}

