#include <iostream>

#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"


#include <string>
#include <sstream>
#include <iomanip>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "ETH_block_dataset_creation"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";
static const char* filter_contract_creation_tag = FILTER_NAME ":contract_creation";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void run_(const DBAccess *access) {
    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    // define tags for all entry types that will be created
    const char* new_tags_block[] = {"ETH", "blocks.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_b_to_bc[] = {"ETH", "block_to_blockchain.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx[] = {"ETH", "tx.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_b[] = {"ETH", "tx_to_block.csv", "CROSSCHAIN_DATASET", ""};

    if (d.HasParseError()) return fail_(access, "parse error");

    if (!d.HasMember("hash")) return fail_(access, "no member \"hash\"");
    auto &hash = d["hash"];

    if (!hash.IsString()) return fail_(access, "hash was not a string");
    string block_hash = hash.GetString();

    if (!d.HasMember("number")) return fail_(access, "no member \"number\"");
    auto &block_number_raw = d["number"];
    if (!block_number_raw.IsString()) return fail_(access, "block number was not a string");
    stringstream block_num_ss;
    block_num_ss << hex << block_number_raw.GetString();
    uint64_t block_num_int;
    block_num_ss >> block_num_int;

    if (!d.HasMember("timestamp")) return fail_(access, "no member \"timestamp\"");
    auto &time_raw = d["timestamp"];
    if (!time_raw.IsString()) return fail_(access, "timestamp was not a string");
    stringstream time_raw_ss;
    time_raw_ss << hex << time_raw.GetString();
    uint64_t time_raw_int;
    time_raw_ss >> time_raw_int;

    stringstream block_val_ss;
    block_val_ss << block_hash << "," << block_num_int << "," << time_raw_int;

    string block_val;
    block_val = block_val_ss.str();

    //create the entry for block.csv
    access->make_new_entry.run(&access->make_new_entry, new_tags_block, block_val.c_str(), INITIAL_KEY);

    // create the entry for block_to_blockchain.csv
    string new_val_b_to_bc = block_hash + "," + "ETH";
    access->make_new_entry.run(&access->make_new_entry, new_tags_b_to_bc, new_val_b_to_bc.c_str(), INITIAL_KEY);

    bool inactive = false;

    if (!d.HasMember("transactions")) return fail_(access, "no member \"transactions\"");
    auto &txs = d["transactions"];
    if (!txs.IsArray()) return fail_(access, "transactions was not an array");
    for (auto & tx : txs.GetArray()) {
        if (!tx.HasMember("hash")) return fail_(access, "no member hash");
        auto& txid = tx["hash"];
        if (!txid.IsString()) return fail_(access, "hash was not a string");
        string txid_str = txid.GetString();

        // create the entries for tx_to_block.csv
        string tx_to_block = txid_str + "," + block_hash;
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_b, tx_to_block.c_str(), INITIAL_KEY);

        // create the entries for tx.csv
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx, txid_str.c_str(), INITIAL_KEY);


    } // close tx loop
    
    

    // Add a tag indicating parsing was successful
    if (!inactive)
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
        bool found_eth = false;
        bool found_block = false;

        //make sure the entry includes "ETH" and "block" but not
        //"ETH_block_to_tx:done" or "ETH_block_to_tx:fail"
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "ETH")
                found_eth = true;
            if (string(*tags) == "block")
                found_block = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            if (string(*tags) == filter_contract_creation_tag)
                return false;
            ++tags;
        }
        if (found_eth && found_block)
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

