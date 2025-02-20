#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/regex.hpp>

#include <fstream>
#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"


#include <string>
#include <sstream>
#include <regex>

using namespace std;
using namespace pando;
using namespace rapidjson;

static const char* filter_name = "ShapeShiftConvert";
static const char* filter_done_tag = "ShapeShiftConvert:done";
static const char* filter_fail_tag = "ShapeShiftConvert:fail";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

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
    // Create a new entry with a key being the two transaction IDs
    // The tags being the input and output coins
    //   
    string inputTx;
    string outputTx;
    string inputCoin;
    string outputCoin;
    string timestamp;
    vector<string> values;
    boost::split(values, access->value, boost::is_any_of("\n"));
    for (std::string line: values){
        if (boost::starts_with(line, "inputTxId:")) {
            inputTx = get_after(line, "inputTxId:");
        }
        if (boost::starts_with(line, "outputTxId:")) {
            outputTx = get_after(line, "outputTxId:");
        }
        if (boost::starts_with(line, "inputCoin:")){
            inputCoin = get_after(line, "inputCoin:");
        }
        if (boost::starts_with(line, "outputCoin:")){
            outputCoin = get_after(line, "outputCoin:");
        }
        if (boost::starts_with(line, "timestamp:")){
            timestamp = line;
        }
    }
    if (inputTx.size() <= 1) {
        access->add_tag.run(&access->add_tag, filter_fail_tag);
        return fail_(access, "Did not have input tx id!");
    }
    if (outputTx.size() <= 1) {
        access->add_tag.run(&access->add_tag, filter_fail_tag);
        return fail_(access, "Did not have output tx id!");
    }

    vtx_t src_key;
    stringstream ss;
    ss << inputTx.substr(0,15);
    ss >> hex >> src_key;

    vtx_t out_key;
    stringstream ss2;
    ss2 << outputTx.substr(0,15);
    ss2 >> hex >> out_key;
    

    const char* new_tags[] = {inputCoin.c_str(), outputCoin.c_str(),filter_done_tag, timestamp.c_str(), ""};

    
    uint32_t input_key;
    uint32_t output_key;
    string input_str;
    string output_str;
    const char* inputC = inputCoin.c_str();
    const char* outputC = outputCoin.c_str();
    auto blockchain_key_test = get_blockchain_key(inputC);
    if (blockchain_key_test != (uint32_t)-1) {
        input_key = blockchain_key_test;
        input_str.assign(inputC);
    }
    else{
        input_key = 0;
    }

    auto blockchain_key_test_2 = get_blockchain_key(outputC);
    if (blockchain_key_test_2 != (uint32_t)-1) {
        output_key = blockchain_key_test_2;
        output_str.assign(outputC);
    }
    else {
        output_key = 0;
    }

    
    chain_info_t chain_info = pack_chain_info(input_key, SHAPESHIFT_KEY, output_key);
    dbkey_t new_key = {chain_info, src_key, out_key};
    access->make_new_entry.run(&access->make_new_entry, new_tags, access->value, new_key);
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
        bool foundShapeShiftRaw = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "ShapeShift_tx_raw")
                foundShapeShiftRaw = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (foundShapeShiftRaw){
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



