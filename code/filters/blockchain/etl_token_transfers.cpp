#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/regex.hpp>


#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include <sstream>
#include <iomanip>

#include <string>
#include <sstream>
#include <regex>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "etl_token_transfers"
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

string get_after(string og_str, string search_str) {
    // boost split_regex is deprecated and causing seg faults, so do this ourselves
    size_t ind = og_str.find(search_str);
    if (ind == std::string::npos) {
        return "";
    }
    string new_str = og_str.substr(ind+search_str.size(), og_str.size());
    return new_str;
}

void run_(const DBAccess *access) {
    // Create a new entry with a key being the transaction id
    // Where the value is a newline separated list of
    // - Token Address
    // - TX Hash
    // - Value
    // - Block
    string address;
    string transaction_hash;
    vector<string> values;
    boost::split(values, access->value, boost::is_any_of("\n"));
    for (std::string line: values){
        if (boost::starts_with(line, "token_address:")) {
            address = get_after(line, "token_address:");
            if (address == "") {
                access->add_tag.run(&access->add_tag, filter_fail_tag);
                return fail_(access, "Did not have a token address!");
            }
        }
        if (boost::starts_with(line, "transaction_hash:")) {
            transaction_hash = get_after(line, "transaction_hash:");
            if (transaction_hash == "") {
                access->add_tag.run(&access->add_tag, filter_fail_tag);
                return fail_(access, "Did not have a transaction hash!");
            }
        }
    }

    vtx_t src_key;
    stringstream ss;
    ss << transaction_hash.substr(2,15);
    ss >> hex >> src_key;

    const char* new_tags[] = {address.c_str(), "ethereum_etl_token_transfer_parsed", transaction_hash.c_str(), ""};
    chain_info_t eth_key = pack_chain_info(ETH_KEY, ETL_TX_TOKEN_KEY, 0);
    dbkey_t new_key = {eth_key, src_key, 0};
    string new_val = access->value;
    access->make_new_entry.run(&access->make_new_entry, new_tags, new_val.c_str(), new_key);

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
        bool found_etl_tokens = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "ethereum_etl_token_transfer")
                found_etl_tokens = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_etl_tokens){
            return true;
        }
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

