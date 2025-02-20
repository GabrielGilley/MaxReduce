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

static const char* filter_name = "findShapeShiftPairs";
static const char* filter_done_tag = "findShapeShiftPairs:done";
static const char* filter_fail_tag = "findShapeShiftPairs:fail";
static const char* filter_inactive_tag = "findShapeShiftPairs:inactive";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void run_(const DBAccess *access) {
    // Create a new entry with a key being the transaction ids
    // Where the tags are inputCoin output coin "match"
    // Where the vals are 
    // input coin, output coin, amt
    // 
    // One database for round tx vals and one for Shapeshift results
    // The input database is the round tx vals database
    // Loop through round tx vals database based on timestamp
    // Find pairs in the values and compare to shapeshift data
    //
   
    string val_s = access->value;
    
    stringstream check1(val_s);
     
    string tx1;
    string tx2;
    vtx_t tx1_key;
    vtx_t tx2_key;


    if(val_s.find('\n') != std::string::npos){
        getline(check1, tx1, '\n');
        getline(check1, tx2, '\n');
    }    
    stringstream ss;
    ss << tx1;
    ss >> tx1_key;
 
    stringstream ss2;
    ss2 << tx2;
    ss2 >> tx2_key;

     
    auto old_tags = access->tags;
    const char * Coin1 = NULL;
    const char * Coin2 = NULL;
    int i = 0;
    while (old_tags[0][0] != '\0') {
        if(string(old_tags[0])!= "rounded" && string(old_tags[0]) != "out_val_usd" && string(old_tags[0]) != "MERGED"){
            if(i == 0){
                Coin1=old_tags[0];
                i ++;
            }else{
                Coin2=old_tags[0];
                i =0;
            }
            
        }
        ++old_tags;
    }

    uint32_t input_key;
    uint32_t output_key;
    string input_str;
    string output_str;
 
    uint32_t blockchain_key_test;
    if(Coin1 != NULL)
        blockchain_key_test = get_blockchain_key(Coin1);
    else
        blockchain_key_test = (uint32_t) -1;
    if (blockchain_key_test != (uint32_t)-1) {
        input_key = blockchain_key_test;
        input_str.assign(Coin1);
    }
    else{
        input_key = 0;
    }

    uint32_t blockchain_key_test_2;
    if(Coin2 != NULL)
        blockchain_key_test_2 = get_blockchain_key(Coin2);
    else
        blockchain_key_test_2 = (uint32_t) -1;
    if (blockchain_key_test_2 != (uint32_t)-1) {
        output_key = blockchain_key_test_2;
        output_str.assign(Coin2);
    }
    else {
        output_key = 0;
    }
    
    chain_info_t chain_info_1 = pack_chain_info(input_key, SHAPESHIFT_KEY, output_key);

    chain_info_t chain_info_2 = pack_chain_info(output_key, SHAPESHIFT_KEY, input_key);
 
    char *shapeShift_entry_1 = nullptr;
    char *shapeShift_entry_2 = nullptr;  
    char *shapeShift_entry_3 = nullptr;
    char *shapeShift_entry_4 = nullptr;   
 
    dbkey_t search_key = {chain_info_1, tx1_key, tx2_key};
    dbkey_t search_key_2 = {chain_info_1, tx2_key, tx1_key};
    dbkey_t search_key_3 = {chain_info_2, tx1_key, tx2_key};
    dbkey_t search_key_4 = {chain_info_2, tx2_key, tx1_key};
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &shapeShift_entry_1);
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key_2, &shapeShift_entry_2);
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key_3, &shapeShift_entry_3);
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key_4, &shapeShift_entry_4);

    
 
    if(shapeShift_entry_1 != nullptr || shapeShift_entry_2 != nullptr || 
            shapeShift_entry_3 != nullptr || shapeShift_entry_4 != nullptr){
        access->add_tag.run(&access->add_tag, "ShapeShift_transaction_found");
    } else {
        access->add_tag.run(&access->add_tag, filter_inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, filter_inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key_2, filter_inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key_3, filter_inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key_4, filter_inactive_tag);
        return;
    }

    if(shapeShift_entry_1 != nullptr)
        free(shapeShift_entry_1);
    if(shapeShift_entry_2 != nullptr)
        free(shapeShift_entry_2);
    if(shapeShift_entry_3 != nullptr)
        free(shapeShift_entry_3);
    if(shapeShift_entry_4 != nullptr)
        free(shapeShift_entry_4);

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

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "out_val_usd")
                found_usd = true;
            if (string(*tags) == "rounded")
                found_rounded = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            if (string(*tags) == filter_inactive_tag)
                return false;
            ++tags;
        }

        if (found_usd && found_rounded){
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

