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

#define FILTER_NAME "BTC_block_dataset_creation"
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

string to_hex_string(size_t hash_value) {
    stringstream hex_stream;
    hex_stream << hex << hash_value;
    return hex_stream.str();
}

void run_(const DBAccess *access) {
    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    // define tags for all entry types that will be created
    const char* new_tags_addr[] = {"BTC", "address.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_block[] = {"BTC", "blocks.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_b_to_bc[] = {"BTC", "block_to_blockchain.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx[] = {"BTC", "tx.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_b[] = {"BTC", "tx_to_block.csv", "CROSSCHAIN_DATASET", ""};

    if (d.HasParseError()) return fail_(access, "parse error");

    if (!d.HasMember("hash")) return fail_(access, "no member \"hash\"");
    auto &hash = d["hash"];

    if (!hash.IsString()) return fail_(access, "hash was not a string");
    string block_hash = hash.GetString();

    if (!d.HasMember("height")) return fail_(access, "no member \"height\"");
    auto &block_number_raw = d["height"];
    if (!block_number_raw.IsInt()) return fail_(access, "block number was not an int");

    if (!d.HasMember("time")) return fail_(access, "no member \"time\"");
    auto &time_raw = d["time"];
    if (!time_raw.IsInt()) return fail_(access, "time was not an int");

    stringstream block_val_ss;
    block_val_ss << block_hash << "," << block_number_raw.GetInt() << "," << time_raw.GetInt();

    string block_val;
    block_val = block_val_ss.str();

    //create the entry for block.csv
    access->make_new_entry.run(&access->make_new_entry, new_tags_block, block_val.c_str(), INITIAL_KEY);

    // create the entry for block_to_blockchain.csv
    string new_val_b_to_bc = block_hash + "," + "BTC";
    access->make_new_entry.run(&access->make_new_entry, new_tags_b_to_bc, new_val_b_to_bc.c_str(), INITIAL_KEY);

    if (!d.HasMember("tx")) return fail_(access, "no member \"tx\"");
    auto &txs = d["tx"];
    if (!txs.IsArray()) return fail_(access, "txs was not an array");
    for (auto & tx : txs.GetArray()) {
        if (!tx.HasMember("txid")) return fail_(access, "no member txid");
        auto& txid = tx["txid"];
        if (!txid.IsString()) return fail_(access, "txid was not a string");
        string txid_str = txid.GetString();

        // create the entries for tx_to_block.csv
        string tx_to_block = txid_str + "," + block_hash;
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_b, tx_to_block.c_str(), INITIAL_KEY);

        // create the entries for tx.csv
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx, txid_str.c_str(), INITIAL_KEY);

        if (!tx.HasMember("vout")) return fail_(access, "no member vout");
        auto& vouts = tx["vout"];
        if (!vouts.IsArray()) return fail_(access, "vout is not an array");
        for (auto & vout: vouts.GetArray()) {
            if (!vout.HasMember("scriptPubKey")) return fail_(access, "no member scriptPubKey");
            auto &scriptPubKey = vout["scriptPubKey"];
            if (!scriptPubKey.IsObject()) return fail_(access, "scriptPubKey was not an object");
            if (!scriptPubKey.HasMember("addresses")) {
                // This one is different -- it's okay if it doesn't have an addres, so we just continue
                continue;
            }
            auto& addresses = scriptPubKey["addresses"];
            if (!addresses.IsArray()) return fail_(access, "addresses was not an array");
            for (auto & addr : addresses.GetArray()) {
                if (!addr.IsString()) return fail_(access, "address was not a string");
                string addr_str = addr.GetString();

                // create the entries for address.csv
                chain_info_t addr_ci = pack_chain_info(BTC_KEY, ADDR_KEY, 0);
                // hash the address and conver to hex (int) to be used as a key
                vtx_t addr_key;
                std::hash<std::string> hasher;
                auto hashed_value_std = hasher(addr_str);
                string hex_std = to_hex_string(hashed_value_std);
				stringstream hash_ss;
                hash_ss << hex_std.substr(0,15);
                hash_ss >> hex >> addr_key;
                dbkey_t new_addr_key {addr_ci, addr_key, 0};

                access->make_new_entry.run(&access->make_new_entry, new_tags_addr, addr_str.c_str(), new_addr_key);
            }


        } // close vout loop


    } // close tx loop
    
    

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
        bool found_btc = false;
        bool found_block = false;

        //make sure the entry includes "BTC" and "block" but not
        //"BTC_block_to_tx:done" or "BTC_block_to_tx:fail"
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "BTC")
                found_btc = true;
            if (string(*tags) == "block")
                found_block = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_btc && found_block)
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

