#include "helpers.hpp"
#include <sstream>
#include <algorithm>
#include <stdexcept>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "read_to_seq"
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";


template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_fail_tag, const char* filter_name, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void shortread_to_seq(const DBAccess *access, const char* filter_fail_tag, const char* filter_done_tag) {
    
    // Create new DB entries for each transaction
    string val = string(access->value);
    vector<string> lines;
    split_by_newline(val, &lines);
    const char* new_tags[] = {"seq", "short", ""};
    val = lines[1];
    val.erase(std::remove_if(val.begin(), val.end(), [](unsigned char c) { return std::isspace(c); }), val.end());    
    const char* new_value = val.c_str();
    if (lines[1].empty())
    {
        access->add_tag.run(&access->add_tag, filter_fail_tag);
    }
    string key_str = convert_wgsim_id_to_key(lines[0]);
    
    size_t midpoint = key_str.length() / 2;
    std::string first_half = key_str.substr(0, midpoint);
    std::string second_half = key_str.substr(midpoint);  
    
    vtx_t b_key = stoll(first_half);
    vtx_t c_key = stoll(second_half);
    dbkey_t new_key = {SEQ, b_key, c_key};
    access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}


bool should_run_shortread_to_seq(const DBAccess* access, const char* filter_fail_tag, const char* filter_done_tag) {
    auto tags = access->tags;
    bool found_read = false;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == "shortread")
            found_read = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_read)
        return true;
    return false;
}


void run_(const DBAccess *access) {
    return shortread_to_seq(access, filter_fail_tag, filter_done_tag);
}


// Fit the Pando API
extern "C" {
    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access) {
        return should_run_shortread_to_seq(access, filter_fail_tag, filter_done_tag);
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: FILTER_NAME,
        filter_type: SINGLE_ENTRY,
        should_run: &should_run,
        init: nullptr,
        destroy: nullptr,
        run: &run
    };
}
