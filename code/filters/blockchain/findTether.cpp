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

#define FILTER_NAME "find_tether"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";
//static const char* inactive_tag = FILTER_NAME ":inactive";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}


void run_(const DBAccess *access) {
    // Read the values from omni
    // Read property ID
    // Add USDT tag to entry if property ID is 31

    

    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, "cannot parse");
    if (!d.HasMember("propertyid")) return fail_(access, "omni has no member \"propertyid\"");
    if (d["propertyid"] == 31){
        access->add_tag.run(&access->add_tag, "USDT");
    }
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
        bool omniComplete = false;
        bool omniTx = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "OMNI")
                omniComplete = true;
            if (string(*tags) == "tx")
                omniTx = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (omniComplete && omniTx){
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



