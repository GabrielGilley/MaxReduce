#include <iostream>
#include <string>
#include "filter.hpp"
#include "dbkey.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

float get_exchange_rate(time_t timestamp, const DBAccess *access, const char* inactive_tag, vtx_t bc_key) {
    //generate a tm object from the timestamp so we can floor the hours,
    //minute, etc. because we only care about exchange rate at the fidelity of
    //a given day
    struct tm *tm;
    tm = gmtime(&timestamp);
    tm->tm_sec = 0;
    tm->tm_min = 0;
    tm->tm_hour = 0;
    time_t ts_key = mktime(tm) - timezone;

    //create the search key
    chain_info_t ci = pack_chain_info(bc_key, EXCHANGE_RATE_KEY, 0);
    dbkey_t search_key = {ci, ts_key, 0};
    char* exchange_rate_ret = nullptr;
    string exchange_rate_str;
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &exchange_rate_ret);
    if (exchange_rate_ret == nullptr) {
        access->add_tag.run(&access->add_tag, inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, inactive_tag);
        return -1;
    } else {
        exchange_rate_str.assign(exchange_rate_ret);
        free(exchange_rate_ret);
    }
    float exchange_rate = stof(exchange_rate_str);
    return exchange_rate;
}

// NOTE: This isn't a great function to use in the should_run, as it would
// iterate over the tags multiple times. However, it can be useful if you're
// just checking for the existence of a single tag
bool has_tag(const DBAccess* access, string tag) {
    auto tags = access->tags;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == tag)
            return true;
        ++tags;
    }
    return false;
}
