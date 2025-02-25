#include <iostream>
#include <exception>
#include <string>

#include "address.hpp"
#include "par_db.hpp"

#include "util.hpp"

using namespace std;

using namespace pando;

int main_(int argc, char **argv) {
    if (argc < 3) {
        cerr << "Usage: pando db-addr comand [args...]\n"
            "\n"
            "Parameters:\n"
            "  db-addr : the address to connect the Pando client to\n"
            "  command : the command to run, with appropriate args\n"
            "\n"
            "Addresses are of the form: IPv4-string,ID\n"
            "  IPv4-string : a period separated IP address, e.g., 1.2.3.4\n"
            "  ID : a small integer between 0 and 199\n"
            "\n"
            "Available commands are:\n"
            "  db_size                    : return the size of the DB\n"
            "  ring_size                  : return the size of the ring\n"
            "  num_neighbors              : return the number of neighbors\n"
            "  add_filter_dir filter-dir  : add the directory of .so files as filter\n"
            "  install_filter filter-name : install a new filter\n"
            "  add_db_file    file        : load entries from a text file\n"
            "  process                    : process a single round of filters\n"
            "  clear_filters              : Uninstall all filters on all DBs\n"
            "  export_db      export-dir  : export a pando db to a directory\n"
            "  import_db      import-dir  : import pando db files from  a directory\n"
            "  print_entries              : (DEBUGGING) tell a pardb to print its entries\n"
            "  stage_close                : (DEBUGGING) -- force a stage close\n"
            "  check_at_barrier           : (DEBUGGING) -- call the check_at_barrier function\n"
            ;
        return 1;
    }

    elga::ZMQAddress db_addr = get_zmq_addr(argv[1]);

    ParDBClient c { db_addr };

    string cmd { argv[2] };

    if (cmd == "db_size") {
        cout << c.db_size() << endl;
    }
    else if (cmd == "ring_size") {
        cout << c.ring_size() << endl;
    }
    else if (cmd == "num_neighbors") {
        cout << c.num_neighbors() << endl;
    }
    else if (cmd == "add_filter_dir") {
        if (argc < 4) throw runtime_error("Missing argument");
        string filter_dir {argv[3]};
        c.add_filter_dir(filter_dir);
    }
    else if (cmd == "install_filter") {
        if (argc < 4) throw runtime_error("Missing argument");
        string filter_name {argv[3]};
        c.install_filter(filter_name);
    }
    else if (cmd == "add_db_file") {
        if (argc < 4) throw runtime_error("Missing argument");
        string file_name {argv[3]};
        c.add_db_file(file_name);
    }
    else if (cmd == "process") {
        c.process();
    }
    else if (cmd == "clear_filters") {
        c.clear_filters();
    } else if (cmd == "export_db") {
        if (argc < 4) throw runtime_error("Missing argument");
        string export_dir {argv[3]};
        c.export_db(export_dir);
    }
    else if (cmd == "import_db") {
        if (argc < 4) throw runtime_error("Missing argument");
        string import_dir {argv[3]};
        c.import_db(import_dir);
    }
    else if (cmd == "stage_close") {
        c.stage_close();
    }
    else if (cmd == "check_at_barrier") {
        c.check_at_barrier();
    }
    else if (cmd == "print_entries"){
        c.print_entries();
    }
    else throw runtime_error("Unknown command");

    return 0;
}

int main(int argc, char **argv) {
    elga::ZMQChatterbox::Setup();

    int ret;

    try {
        ret = main_(argc, argv);
    } catch(...) {
        elga::ZMQChatterbox::Teardown();
        throw;
    }

    elga::ZMQChatterbox::Teardown();

    return ret;
}
