#include <iostream>

#include "filter.hpp"
#include "dbkey.h"
#include <algorithm>

#include <sstream>
#include <math.h>

#include <string>

using namespace std;
using namespace pando;

#define FILTER_NAME "round_tx_vals"
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

void make_entry(const DBAccess *access, vtx_t rounded_timestamp, const char* new_tags[], double rounded_val) {
    //FIXME i think this breaks if there are two transactions in the same block
    //for the same (or similar) amounts (in particular, two outputs with same
    //amount for BTC
    chain_info_t ci = pack_chain_info(USD_KEY, OUT_VAL_ROUNDED_KEY, 0);
    vtx_t b_key = rounded_timestamp;
    vtx_t c_key = rounded_val;
    dbkey_t new_key_rounded = {ci, b_key, c_key};
    access->make_new_entry.run(&access->make_new_entry, new_tags, to_string((access->key).c).c_str(), new_key_rounded);
}

void run_(const DBAccess *access) {
    /*
     * For this filter, we use the term "rounding" pretty loosely, since this
     * will be used to associate transactions between blockchains (i.e. to
     * detect exchanging BTC for ETH). We need to round the transaction value
     * to TX_ROUND_AMT while also including the next highest and next lowest
     * value. So, if TX_ROUND_AMT is $10, and the transaction value is $53, there
     * will be an entry created with value $50 (round amount), one with value
     * $60 (next highest value) and one with value $40 (next lowest value).
     * Similarly, TIME_ROUND_AMT is used to round timestamps of the transaction.
     * So, if TIME_ROUND_AMT is set to 1 hour, and the tx time was 13:18, there
     * would be new entries created at 13:00, 14:00, and 12:00.
     *
     * So, the previous example would result in the following 9 new entries:
     * $40 @ 12:00
     * $40 @ 13:00
     * $40 @ 14:00
     * $50 @ 12:00
     * $50 @ 13:00
     * $50 @ 14:00
     * $60 @ 12:00
     * $60 @ 13:00
     * $60 @ 14:00
     * */
    double TX_ROUND_AMT = 10; //in USD
    double TIME_ROUND_AMT = 3600; //seconds

    string val_s = access->value;
    double val = stod(val_s);

    //Round tx amount to nearest multiple of TX_ROUND_AMT
    double rounded_val = floor(((val + TX_ROUND_AMT/2) / TX_ROUND_AMT)) * TX_ROUND_AMT;
    double next_highest = rounded_val + TX_ROUND_AMT;
    double next_lowest = rounded_val - TX_ROUND_AMT;
    if (next_lowest < 0) {
        next_lowest = 0;
    }
    
    //Round time amount to nearest multiple of TIME_ROUND_AMT
    vtx_t cur_timestamp = (access->key).b;
    double rounded_timestamp = floor(((cur_timestamp + TIME_ROUND_AMT/2) / TIME_ROUND_AMT)) * TIME_ROUND_AMT;
    double next_highest_timestamp = rounded_timestamp + TIME_ROUND_AMT;
    double next_lowest_timestamp = rounded_timestamp - TIME_ROUND_AMT;

    //append a "rounded" tag, but reuse this entry's tag otherwise
    auto old_tags = access->tags;
    vector<const char*> new_tags_v;
    while ((*old_tags)[0] != '\0') {
        new_tags_v.push_back(*old_tags);
        ++old_tags;
    }
    new_tags_v.push_back("rounded");
    //Make sure the emptry string tag is at the back
    new_tags_v.erase(remove(new_tags_v.begin(), new_tags_v.end(), ""), new_tags_v.end());
    new_tags_v.push_back("");
    const char* new_tags[new_tags_v.size()];
    size_t i = 0;
    for (auto ele : new_tags_v) {
        new_tags[i] = move(ele);
        i++;
    }

    //Make the entry for the closest rounding at each timestamp
    make_entry(access, rounded_timestamp, new_tags, rounded_val);
    make_entry(access, next_lowest_timestamp, new_tags, rounded_val);
    make_entry(access, next_highest_timestamp, new_tags, rounded_val);
    //Make the entry for the next lowest rounding at each timestamp
    make_entry(access, rounded_timestamp, new_tags, next_lowest);
    make_entry(access, next_lowest_timestamp, new_tags, next_lowest);
    make_entry(access, next_highest_timestamp, new_tags, next_lowest);
    //Make the entry for the next highest rounding at each timestamp
    make_entry(access, rounded_timestamp, new_tags, next_highest);
    make_entry(access, next_lowest_timestamp, new_tags, next_highest);
    make_entry(access, next_highest_timestamp, new_tags, next_highest);

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
        bool found_usd = false;
        bool found_rounded = false;

        //make sure the entry includes "BTC" or "ZEC" and "tx" but not
        //"BTC_tx_out_edges:done" or "BTC_tx_out_edges:fail"
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "out_val_usd")
                found_usd = true;
            if (string(*tags) == "rounded")
                found_rounded = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_usd && !found_rounded)
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

