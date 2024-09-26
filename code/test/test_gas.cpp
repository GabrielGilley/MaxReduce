#include "test.hpp"
#include "alg_db.hpp"
#include "gas_filter.hpp"
#include <algorithm>

using namespace std;
using namespace pando;

bool filter_yes_run([[maybe_unused]] const DBAccess* acc) {
    return true;
}

using T = int;
class StandaloneGAS : public GASFilter<T, T> {
    protected:
        T G(T a, [[maybe_unused]] T b) { return a; }
        T A(T v, [[maybe_unused]] T a) {
            //take in the newly gathered value
            return v;
        }
        T S(T v) { return v; }
        T gather_init([[maybe_unused]] DBAccess* acc) {
            // This will always result in 0 being set
            return 0;
        }
        T init([[maybe_unused]] DBAccess* acc) {
            // Before anything is run values should be 1
            return 1;
        }
        void save_output(DBAccess* acc, T val) {
            const char* tags[] = {"OUTPUT", ""};
            string val_s = to_string(val);
            acc->make_new_entry.run(&acc->make_new_entry, tags, val_s.c_str(), INITIAL_KEY);
        }
    public:
        using GASFilter<T,T>::GASFilter;
        StandaloneGAS(DBAccess* acc [[maybe_unused]]) : GASFilter<T,T>::GASFilter(acc) { }
};

void standalone_gas_run(void* state) {
    StandaloneGAS& gas = *(StandaloneGAS*)state;
    gas.run();
}
void* standalone_gas_init(DBAccess* access) {
    StandaloneGAS* gas = new StandaloneGAS(access);
    return (void*)gas;
}
void standalone_gas_destroy([[maybe_unused]] void* state) { 
    delete (StandaloneGAS*)state;
}

TEST(standalone_gas) {
    AlgDB db;

    FilterInterface i {
        filter_name: "STANDALONE_GAS",
        filter_type: GROUP_ENTRIES,
        should_run: &filter_yes_run,
        init: &standalone_gas_init,
        destroy: &standalone_gas_destroy,
        run: &standalone_gas_run
    };

    db.install_filter(make_shared<Filter>(&i));

    {
        dbkey_t key {1,1,1};
        DBEntry<> e; e.set_key(key); db.add_entry(move(e));
    }
    {
        dbkey_t key {1,1,2};
        DBEntry<> e; e.set_key(key); db.add_entry(move(e));
    }

    EQ(db.size(), 2);

    db.process();

    EQ(db.size(), 4);

    for (auto & [key, entry] : db.entries()) {
        if (is_random_key(key)) {
            EQ(entry.value(), "1");
            EQ(entry.has_tag("OUTPUT"), true);
        }
    }
    
    TEST_PASS
}

// GAS filter that terminates after so many iterations
using T = int;
class IterationStopGAS : public GASFilter<T, T> {
    protected:
        T G(T a, [[maybe_unused]] T b) { return a; }
        T A(T v, [[maybe_unused]] T a) {
            //take in the newly gathered value
            return v;
        }
        T S([[maybe_unused]] T v) {
            if (iter > 20) {
                //stop after max 20 iterations
                return 5;
            }

            if (v == 10)
                return 1;
            return 10;
        }
        T gather_init([[maybe_unused]] DBAccess* acc) {
            // This will always result in 0 being set
            return 0;
        }
        T init([[maybe_unused]] DBAccess* acc) {
            // Before anything is run values should be 1
            return 1;
        }
        void save_output(DBAccess* acc, T val) {
            const char* tags[] = {"OUTPUT", ""};
            string val_s = to_string(val);
            acc->make_new_entry.run(&acc->make_new_entry, tags, val_s.c_str(), INITIAL_KEY);
        }
    public:
        using GASFilter<T,T>::GASFilter;
        IterationStopGAS(DBAccess* acc [[maybe_unused]]) : GASFilter<T,T>::GASFilter(acc) { }
};

void iteration_stop_gas_run(void* state) {
    IterationStopGAS& gas = *(IterationStopGAS*)state;
    gas.run();
}
void* iteration_stop_gas_init(DBAccess* access) {
    IterationStopGAS* gas = new IterationStopGAS(access);
    return (void*)gas;
}
void iteration_stop_gas_destroy([[maybe_unused]] void* state) { 
    delete (IterationStopGAS*)state;
}

TEST(iteration_stop_gas) {
    AlgDB db;

    FilterInterface i {
        filter_name: "ITERATION_STOP_GAS",
        filter_type: GROUP_ENTRIES,
        should_run: &filter_yes_run,
        init: &iteration_stop_gas_init,
        destroy: &iteration_stop_gas_destroy,
        run: &iteration_stop_gas_run
    };

    db.install_filter(make_shared<Filter>(&i));

    {
        dbkey_t key {1,1,1};
        DBEntry<> e; e.set_key(key); db.add_entry(move(e));
    }
    {
        dbkey_t key {1,1,2};
        DBEntry<> e; e.set_key(key); db.add_entry(move(e));
    }

    EQ(db.size(), 2);

    db.process();

    EQ(db.size(), 4);

    size_t count = 0;
    for (auto & [key, entry] : db.entries()) {
        if (is_random_key(key)) {
            EQ(entry.value(), "5");
            EQ(entry.has_tag("OUTPUT"), true);
            count++;
        }
    }
    EQ(count > 0, true);
    
    TEST_PASS
}

// GAS filter that terminates after so many iterations
using T = int;
class MaxValGAS : public GASFilter<T, T> {
    protected:
        T G(T a, T b) { return max(a,b); }
        T A(T v, T a) { return max(v, a); }
        T S(T v) { return v; }
        T gather_init([[maybe_unused]] DBAccess* acc) { return 0; }
        T init([[maybe_unused]] DBAccess* acc) {
            // We need to request the value for the entry:
            //  (2, <VertexID>, 0)
            vtx_t vertex_id = acc->group->vtx;
            
            dbkey_t key {2, vertex_id, 0};
            char* init_val_cstr = nullptr;
            acc->get_entry_by_key.run(&acc->get_entry_by_key, key, &init_val_cstr);
            if (init_val_cstr == nullptr) throw runtime_error("Unable to find vertex information");

            T init_val = (T)strtoll(init_val_cstr, NULL, 10);

            // Next, make the output so that we can save to it later
            dbkey_t out_key {3, vertex_id, 0};
            const char* tags[] = {"OUTPUT", ""};
            acc->make_new_entry.run(&acc->make_new_entry, tags, init_val_cstr, out_key);

            free(init_val_cstr);
            return init_val;
        }
        void save_output(DBAccess* acc, T val) {
            string val_s = to_string(val);
            dbkey_t out_key {3, acc->group->vtx, 0};
            acc->update_entry_val.run(&acc->update_entry_val, out_key, val_s.c_str());
        }
    public:
        using GASFilter<T,T>::GASFilter;
        MaxValGAS(DBAccess* acc [[maybe_unused]]) : GASFilter<T,T>::GASFilter(acc) { }
};

void max_val_gas_run(void* state) {
    MaxValGAS& gas = *(MaxValGAS*)state;
    gas.run();
}
void* max_val_gas_init(DBAccess* access) {
    MaxValGAS* gas = new MaxValGAS(access);
    return (void*)gas;
}
void max_val_gas_destroy([[maybe_unused]] void* state) { 
    delete (MaxValGAS*)state;
}
bool max_val_should_run([[maybe_unused]] const DBAccess* acc) {
    if (acc->key.a == 1)
        return true;
    return false;
}

TEST(max_val) {
    AlgDB db;

    FilterInterface i {
        filter_name: "MAX_VAL_GAS",
        filter_type: GROUP_ENTRIES,
        should_run: &max_val_should_run,
        init: &max_val_gas_init,
        destroy: &max_val_gas_destroy,
        run: &max_val_gas_run
    };

    db.install_filter(make_shared<Filter>(&i));

    /* Create the following graph
           (2)
          /   \
       (7)    (3)
        |      |
       (6)    (8)
         \    /
           (1)

           Result:
           (2)
          /   \
       (7)    (3)
        |      |
       (7)    (8)
         \    /
           (8)
    */

    // Create vertex values
    { dbkey_t key {2,1,0}; DBEntry<> e; e.value() = to_string(2); e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {2,2,0}; DBEntry<> e; e.value() = to_string(7); e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {2,3,0}; DBEntry<> e; e.value() = to_string(3); e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {2,4,0}; DBEntry<> e; e.value() = to_string(6); e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {2,5,0}; DBEntry<> e; e.value() = to_string(8); e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {2,6,0}; DBEntry<> e; e.value() = to_string(1); e.set_key(key); db.add_entry(move(e)); }

    // Create edges
    { dbkey_t key {1,1,2}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {1,1,3}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {1,2,4}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {1,3,5}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {1,4,6}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }
    { dbkey_t key {1,5,6}; DBEntry<> e; e.set_key(key); db.add_entry(move(e)); }

    db.process();

    vector<string> vals;
    for (auto & [key, entry] : db.entries()) {
        if (key.a == 3) {
            EQ(entry.has_tag("OUTPUT"), true);
            vals.emplace_back(entry.value());
        }
    }
    
    sort(vals.begin(), vals.end());
    vector<string> res {{"2", "3", "7", "7", "8", "8"}};
    EQ(vals.size(), res.size());
    for (size_t idx = 0; idx < vals.size(); ++idx)
        EQ(vals[idx], res[idx]);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(standalone_gas)
    RUN_TEST(iteration_stop_gas)
    RUN_TEST(max_val)
    elga::ZMQChatterbox::Teardown();
TESTS_END
