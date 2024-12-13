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


template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_fail_tag, const char* filter_name, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void eth_based_block_dataset_creation(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_fail_tag, const char* filter_done_tag) {
    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    // define tags for all entry types that will be created
    const char* new_tags_block[] = {crypto_tag, "blocks.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_b_to_bc[] = {crypto_tag, "block_to_blockchain.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx[] = {crypto_tag, "tx.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_b[] = {crypto_tag, "tx_to_block.csv", "CROSSCHAIN_DATASET", ""};

    if (d.HasParseError()) return fail_(access, filter_fail_tag, filter_name, "parse error");

    if (!d.HasMember("hash")) return fail_(access, filter_fail_tag, filter_name, "no member \"hash\"");
    auto &hash = d["hash"];

    if (!hash.IsString()) return fail_(access, filter_fail_tag, filter_name, "hash was not a string");
    string block_hash = hash.GetString();

    if (!d.HasMember("number")) return fail_(access, filter_fail_tag, filter_name, "no member \"number\"");
    auto &block_number_raw = d["number"];
    if (!block_number_raw.IsString()) return fail_(access, filter_fail_tag, filter_name, "block number was not a string");
    stringstream block_num_ss;
    block_num_ss << hex << block_number_raw.GetString();
    uint64_t block_num_int;
    block_num_ss >> block_num_int;

    if (!d.HasMember("timestamp")) return fail_(access, filter_fail_tag, filter_name, "no member \"timestamp\"");
    auto &time_raw = d["timestamp"];
    if (!time_raw.IsString()) return fail_(access, filter_fail_tag, filter_name, "timestamp was not a string");
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
    string new_val_b_to_bc = block_hash + "," + crypto_tag;
    access->make_new_entry.run(&access->make_new_entry, new_tags_b_to_bc, new_val_b_to_bc.c_str(), INITIAL_KEY);

    bool inactive = false;

    if (!d.HasMember("transactions")) return fail_(access, filter_fail_tag, filter_name, "no member \"transactions\"");
    auto &txs = d["transactions"];
    if (!txs.IsArray()) return fail_(access, filter_fail_tag, filter_name, "transactions was not an array");
    for (auto & tx : txs.GetArray()) {
        if (!tx.HasMember("hash")) return fail_(access, filter_fail_tag, filter_name, "no member hash");
        auto& txid = tx["hash"];
        if (!txid.IsString()) return fail_(access, filter_fail_tag, filter_name, "hash was not a string");
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

bool should_run_eth_based_block_dataset_creation(const DBAccess* access, const char* crypto_tag, const char* filter_fail_tag, const char* filter_done_tag) {
    auto tags = access->tags;
    bool found_eth = false;
    bool found_block = false;

    // make sure the entry includes crypto_tag and "block" but not
    // the done or fail tag
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_eth = true;
        if (string(*tags) == "block")
            found_block = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_eth && found_block)
        return true;
    return false;
}
