#include <iostream>

#include "filter.hpp"
#include "dbkey.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

#include <pybind11/pybind11.h>
#include <pybind11/embed.h>

#include <string>

using namespace std;
using namespace pando;
using namespace rapidjson;
namespace py = pybind11;

#define FILTER_NAME "ETH_smart_contract_decode"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: ";
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void run_(const DBAccess *access) {
    //Parse the tx json
    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, "JSON Parse Error");
    if (!d.IsObject()) return fail_(access, "\"entry\" was not an object");

    //Extract the "tx" object
    if (!d.HasMember("tx"))
        return fail_(access, "No member \"tx\"");
    auto &tx = d["tx"];
    if (!tx.IsObject()) return fail_(access, "\"tx\" was not an object");

    //Extract the "tx" object
    if (!tx.HasMember("input"))
        return fail_(access, "No member \"input\"");
    auto &input = tx["input"];
    if (!input.IsString()) return fail_(access, "\"input\" was not a string");

    //If there's no smart contract data, just finish
    string empty_sc = "0x";
    string sc_data = input.GetString();
    if (empty_sc.compare(sc_data) == 0) {
        access->add_tag.run(&access->add_tag, filter_done_tag);
        return;
    }
    //cout << sc_data << endl;
    sc_data = sc_data.substr(2);
    
    //Get the address of the destination smart contract
    if (!tx.HasMember("to"))
        return fail_(access, "No member \"to\"");
    auto &to = tx["to"];
    if (!to.IsString()) {
        return fail_(access, "\"to\" was not a string");
    }
    string contract_address = to.GetString();

    //start the python interpreter
    try {
        py::initialize_interpreter();
    } catch(...) {
        ;
    }
    //py::scoped_interpreter python;
    auto sys = py::module_::import("sys");
    sys.attr("path").attr("insert")(0, "filters"); //FIXME make this path more dynamic
    auto decode_module = py::module::import("decode");
    //cout << sc_data << "\n" << endl;
    string decoded = decode_module.attr("decode_sc_data")(sc_data, contract_address).cast<string>();
    if (decoded.compare("FAIL") == 0) {
        cout << "FAILURE" << endl;
        access->add_tag.run(&access->add_tag, filter_fail_tag);
        return;
    }
    //cout << decoded << endl;
    const char* new_tags[] = {"ETH", "decoded_smart_contract", ""};
    access->make_new_entry.run(&access->make_new_entry, new_tags, decoded.c_str(), INITIAL_KEY);

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
        bool found_tx = false;

        //make sure the entry includes "ETH" and "block" but not
        //"ETH_block_to_tx:done" or "ETH_block_to_tx:fail"
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "ETH")
                found_eth = true;
            if (string(*tags) == "tx")
                found_tx = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_eth && found_tx)
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

