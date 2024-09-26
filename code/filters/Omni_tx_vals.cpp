#include <iostream>

#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "helpers.hpp"
#include <sstream>

#include <string>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "OMNI_tx_vals"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";
static const char* inactive_tag = FILTER_NAME ":inactive";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool rollback([[maybe_unused]] const DBAccess *access) {return true;}


void run_(const DBAccess *access) {
    //Parse the tx a bit further

    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, "cannot parse");




    // Find the transactions in the json
    auto &txid = d["txid"];
    if (!txid.IsString()) return fail_(access, "txid is not a string");
    string txid_s;
    txid_s.assign(txid.GetString());

    vtx_t tx_hash_key;
    stringstream tx_hash_ss;
    tx_hash_ss << hex << txid_s.substr(0,15);
    tx_hash_ss >> tx_hash_key;

    //Lookup the time of the transaction
    if (!d.HasMember("blocktime")) return fail_(access, "omni has no member \"blocktime\"");
    auto &time = d["blocktime"];
    int time1 = time.GetInt();
    string time_s = std::to_string(time1);
    time_t timestamp = stoi(time_s);


    //For each vout value, create a new entry for it
    if (!d.HasMember("amount")) return fail_(access, "omni had no member \"amount\"");
    auto &val1 = d["amount"];
    float val = stof(val1.GetString());
    string val_s = to_string(val);
    const char* new_tags[] = {"OMNI", "out_val_omni", ""};
    chain_info_t ci = pack_chain_info(OMNI_KEY, OUT_VAL_KEY, 0);
    vtx_t b_key = timestamp;
    dbkey_t new_key = {ci, b_key, tx_hash_key};
    access->make_new_entry.run(&access->make_new_entry, new_tags, val_s.c_str(), new_key);

        //Create the equivalent entry converted to USD
    float exchange_rate = get_exchange_rate(timestamp, access, inactive_tag, USDT_KEY);
    if (exchange_rate < 0) {
        return;
    }
    const char* new_tags_usd[] = {"OMNI", "out_val_usd", ""};
    chain_info_t ci_usd = pack_chain_info(USD_KEY, OUT_VAL_KEY, 0);
    dbkey_t new_key_usd = {ci_usd, b_key, tx_hash_key};
    string val_usd_s = to_string(val * exchange_rate);
    access->make_new_entry.run(&access->make_new_entry, new_tags_usd, val_usd_s.c_str(), new_key_usd);
    

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
        bool found_omni = false;
        bool found_tx = false;

        //make sure the entry includes "BTC" or "ZEC" and "tx" but not
        //"BTC_tx_out_edges:done" or "BTC_tx_out_edges:fail"
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "OMNI")
                found_omni = true;
            // I don't think omni needs this one
            if (string(*tags) == "tx")
                found_tx = true;
            if (string(*tags) == inactive_tag)
                return false;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if ((found_omni && found_tx))
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

