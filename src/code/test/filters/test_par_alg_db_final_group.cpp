#include "filter.hpp"

#include "terr.hpp"

#include <chrono>
#include <thread>

#include <iomanip>

/*************************************
NOTE: This is very similar to the test_algdb_simple_group.cpp filter,
however we needed a duplicate to better test the parallel alg db
*************************************/

#define AEQ(x,y)                                                                        \
    do {                                                                                \
        if ((x) != (y)) {                                                               \
            terr << "FAILURE: Expected " << (y) << " instead got " << (x)               \
                << " for " << #x << " in " << __FILE__ << ":" << __LINE__ << "\n";      \
            terr.close(); \
            throw std::runtime_error("test failure");                                   \
            }                                                                           \
    } while(0);

#define FILTER_NAME "test_par_alg_db_final_group"
static const char* filter_name = FILTER_NAME;

void run_(const DBAccess *access) {

    // Note: this will run on the "group" of 2,2,6 and the group of 2,6,0, not
    // on those entries

    auto &out_msgs = *(access->group->out_msgs);
    auto &in_msgs = *(access->group->in_msgs);

    // 'A' = (vertex 1) 2,2,6 ; 'B' = (vertex 5) 2,6,0
    // If iter == 0:
    //      A: send msg to B
    //      B: do nothing, go inactive
    // If iter == 1:
    //      A: ensure no msgs received; go inactive
    //      B: make sure we received 1 msg from A; go inactive

    Terr terr;

    if (access->group->iter == 0) {
        // send a message from 1,1,5 to 1,5,0
        if (access->group->vtx == 2) {
            // We are A
            // Send a message to all 'neighbors'
            int msgval = 1234;
            MData msg { msgval };
            vector<MData> vmsg;
            vmsg.push_back(msg);

            for (auto & ge : access->group->entry.out) {
                itmsg_t itmsg;
                itmsg[ge.n] = vmsg;
                out_msgs[access->group->iter+1] = itmsg;
            }
        } else if (access->group->vtx == 6) {
            // TODO maybe Ensure we have an in-neighbor
            // Do nothing 
            // Make sure we get re-activated
            access->group->state = INACTIVE;
        } else if (access->group->vtx == 888) {
            // Do nothing as well
            // But do not explicitly set INACTIVE(?) FIXME is this correct / useful?
        } else throw runtime_error("Unknown group" + to_string(access->group->vtx));
        return;

    } else if (access->group->iter == 1) {
        if (access->group->vtx == 2) {
            // Make sure we received nothing
            AEQ(in_msgs[access->group->iter][access->group->vtx].size(), 0);
        } else if (access->group->vtx == 6) {
            // Make sure we received the message
            AEQ(in_msgs[access->group->iter][access->group->vtx].size(), 1);
            auto msg = in_msgs[access->group->iter][access->group->vtx][0];
            AEQ(msg.getval<int>(), 1234);
            // Create a new output for it
            const char* const new_tags[] = {"FOO", ""};
            string new_value_str = "BAR";
            dbkey_t new_key {8,8,8};

            access->make_new_entry.run(&access->make_new_entry, new_tags, new_value_str.c_str(), new_key);
        } else if (access->group->vtx == 888) {
        } else throw runtime_error("Unknown group" + to_string(access->group->vtx));

        // Every should be inactive now
        access->group->state = INACTIVE;

    } else throw runtime_error("Unknown iter");

}

// Fit the Pando API
extern "C" {
    /** Pass through the access as state */
    extern void* init(DBAccess* access) { return (void*)access; }
    extern void destroy([[maybe_unused]] void* state) { }

    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access [[maybe_unused]]) {
        // If the new entry has already been created, stop execution
        dbkey_t search_key {8,8,8};
        char* search_entry_val = nullptr;
        access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &search_entry_val);
        if (search_entry_val != nullptr) {
            free(search_entry_val);
            return false;
        }

        // Only run on graph=1
        if (access->key.a == 2)
            return true;
        return false;
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: filter_name,
        filter_type: GROUP_ENTRIES,
        should_run: &should_run,
        init: &init,
        destroy: &destroy,
        run: &run
    };
}

