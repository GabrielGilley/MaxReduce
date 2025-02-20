#include <iostream>

#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include <sstream>
#include <iomanip>

#include <string>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "exchange_rates"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool rollback([[maybe_unused]] const DBAccess *access) {return true;}

void run_(const DBAccess *access) {
    //Iterate over tags and find the timestamp one
    auto tags = access->tags;
    time_t ts;
    bool ts_tag_found = false;
    bool bc_tag_found = false;
    uint32_t blockchain_key;
    string bc_str;
    string ts_str;
    while ((*tags)[0] != '\0') {
        auto blockchain_key_test = get_blockchain_key(*tags);
        if (blockchain_key_test != (uint32_t)-1) {
            blockchain_key = blockchain_key_test;
            bc_str.assign(*tags);
            bc_tag_found = true;
        }
        string tag_str = string(*tags);
        ++tags;
        struct tm tm = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        if (strptime(tag_str.c_str(), "%Y-%m-%d", &tm) == NULL)
            continue;
        ts_tag_found = true;
        ts_str = tag_str;
        ts = mktime(&tm) - timezone;
    }
    if (!ts_tag_found)
        return fail_(access, "Did not have a timestamp tag!");
    if (!bc_tag_found) return fail_(access, "Could not identify a blockchain tag!");
    chain_info_t chain_info = pack_chain_info(blockchain_key, EXCHANGE_RATE_KEY, 0);
    vtx_t time_key = ts;
    dbkey_t new_key = {chain_info, time_key, 0};
    const char* new_tags[] = {bc_str.c_str(), "exchange_rate", ts_str.c_str(), filter_done_tag, ""};
    access->make_new_entry.run(&access->make_new_entry, new_tags, access->value, new_key);

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}

// Fit the Pando API
extern "C" {
    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access) {
        auto tags = access->tags;
        bool found_exchange_rate = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "exchange_rate_raw")
                found_exchange_rate = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_exchange_rate)
            return true;
        return false;
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: filter_name,
        filter_type: SINGLE_ENTRY,
        should_run: &should_run,
        init: nullptr,
        destroy: nullptr,
        run: &run
    };
}

