#include <exception>
#include <locale.h>
#include <ncurses.h>
#include <string>
#include <sys/ioctl.h>

#include "address.hpp"
#include "par_db.hpp"

#include "util.hpp"


using namespace std;
using namespace pando;

class PandoTop : public PandoParticipant {
    private:
        unordered_map<addr_t, tuple<time_t, size_t>> db_state_;
    public:
        PandoTop() : PandoParticipant(elga::ZMQAddress{}, false) {
            sub(STATS);
        }
        void process_msg([[maybe_unused]] msg_type_t type, [[maybe_unused]] zmq_socket_t sock, [[maybe_unused]] const char *data, [[maybe_unused]] const char *end) {
            if (type == STATS) {
                addr_t agent;
                size_t size;
                unpack_single(data, agent);
                unpack_single(data, size);
                time_t now = time(nullptr);
                db_state_[agent] = {now, size};
            } else
                recv_unknown_msg();
        }
        tuple<time_t, size_t> get_state(addr_t addr) {
            if (db_state_.count(addr) == 0) return {0, 0};
            return db_state_[addr];
        }
};

volatile sig_atomic_t *g_shutdown = nullptr;

void si_handle(int) {
    cerr << "[Pando] Ctrl+C caught, shutting down..." << endl;
    if (g_shutdown == nullptr) throw runtime_error("Unable to find shutdown flag");
    *g_shutdown = true;
}
void set_handler() {
    struct sigaction si_handler;
    si_handler.sa_handler = si_handle;
    sigemptyset(&si_handler.sa_mask);
    si_handler.sa_flags = SA_RESTART;
    sigaction(SIGINT, &si_handler, NULL);
}

struct WinPos {
    WINDOW* w;
    int y;
    int x;
    WinPos(WINDOW* nw, int ny, int nx) : w(nw), y(ny), x(nx) { }
    WinPos() : w(nullptr), y(0), x(0) { }
};

struct AgentData {
    WINDOW* w;
    size_t db_size;
    time_t last_seen;
    string str;
    AgentData() : w(nullptr), db_size(0), last_seen(0) { }
};

class PandoData {
    private:
        PandoTop& pp_;
        vector<WINDOW*>& wins_;
        WinPos mesh_, db_, up_;
        int status_;
        unordered_map<addr_t, AgentData> agents_;

        inline static WinPos get_pos_(WINDOW *w) {
            int y, x;
            getyx(w, y, x);
            return {w, y, x};
        }

        const int blk_h_ = 3;
        const int blk_w_ = 22;
        const int blk_pad_ = 1;

        int x_, y_;
        int rows_, cols_;

    public:
        PandoData(PandoTop& pp, vector<WINDOW*>& wins, int rows, int cols) :
                pp_(pp), wins_(wins), status_(0), x_(2), y_(7),
                rows_(rows), cols_(cols) {
        }

        void set_mesh_pos(WINDOW* w) {
            mesh_ = get_pos_(w);
        }
        void write_mesh() {
            size_t known = agents_.size();
            size_t full = pp_.agents().size();
            if (known != full)
                mvwprintw(mesh_.w, mesh_.y, mesh_.x, "%-4ld / %-4ld", known, full);
            else
                mvwprintw(mesh_.w, mesh_.y, mesh_.x, "%-4ld%7s", known, " ");
        }

        void set_db_pos(WINDOW* w) {
            db_ = get_pos_(w);
        }
        void write_db() {
            size_t db_size = 0;
            for (auto& [addr, ad] : agents_) {
                db_size += ad.db_size;
            }

            mvwprintw(db_.w, db_.y, db_.x, "%-11ld", db_size);
        }

        void set_up_pos(WINDOW* w) {
            up_ = get_pos_(w);
        }
        void write_up() {
            wmove(up_.w, up_.y, up_.x);
            wattron(up_.w, COLOR_PAIR(2));
            char status_char = 'X';
            if (status_ % 4 == 0)
                status_char = '-';
            else if (status_ % 4 == 1)
                status_char = '\\';
            else if (status_ % 4 == 2)
                status_char = '|';
            else if (status_ % 4 == 3)
                status_char = '/';
            ++status_;
            if (status_ == 4) status_ = 0;
            waddch(up_.w, status_char);
            wattroff(up_.w, COLOR_PAIR(2));

            wrefresh(up_.w);
        }

        void write_stats() {
            write_mesh();
            write_db();
            wrefresh(db_.w);
        }

        void update() {
            // Process any messages, and register / create windows for all new
            // agents
            printw("START");
            size_t max_recvs = pp_.agents().size()*2;
            for (size_t ctr = 0; ctr < max_recvs && pp_.recv_msg(); ++ctr) {}
            printw("STOP");

            // Now, find any new agents and provide them windows as appropriate
            for (addr_t addr : pp_.agents()) {
                if (agents_.count(addr) == 0) {
                    // This is a new agent
                    AgentData ad;

                    elga::ZMQAddress elga_addr {addr};
                    ad.str = elga_addr.get_addr_str();

                    // Create a new window for it
                    if (x_ + blk_w_ + blk_pad_ >= cols_) {
                        // Move to the next line
                        y_ += blk_h_;
                        x_ = 2;
                    }
                    ad.w = newwin(blk_h_, blk_w_, y_, x_);
                    x_ += blk_w_ + blk_pad_;
                    wattron(ad.w, COLOR_PAIR(3));
                    box(ad.w, 0, 0);
                    wattroff(ad.w, COLOR_PAIR(3));
                    wrefresh(ad.w);

                    agents_[addr] = move(ad);
                }

                // Update the state
                auto [ls, size] = pp_.get_state(addr);
                if (ls != 0 || true) {
                    auto w = agents_[addr].w;
                    agents_[addr].db_size = size;
                    agents_[addr].last_seen = ls;
                    auto age = time(nullptr) - agents_[addr].last_seen;
                    if (age > 999) age = 999;

                    if (age > 60) {
                        wattron(w, COLOR_PAIR(1));
                        box(w, 0, 0);
                        wattroff(w, COLOR_PAIR(1));
                    } else if (age < 10) {
                        wattron(w, COLOR_PAIR(2));
                        box(w, 0, 0);
                        wattroff(w, COLOR_PAIR(2));
                    } else {
                        wattron(w, COLOR_PAIR(3));
                        box(w, 0, 0);
                        wattroff(w, COLOR_PAIR(3));
                    }

                    if (age > 4) {
                        string age_s = to_string(age);
                        size_t age_s_len = age_s.size();

                        mvwprintw(w, 0, blk_w_-age_s_len-1, "%s", age_s.c_str());
                        if (age == 999)
                            wprintw(w, "+");
                    }
                    string addr_str = agents_[addr].str;
                    mvwprintw(w, 0, 1, "%s", addr_str.c_str());
                    mvwprintw(w, 1, 2, "DB size : %-8ld", agents_[addr].db_size);
                    wrefresh(w);
                }
            }

            // Write updates
            write_stats();
            write_up();
        }
};

void top(PandoTop& pp) {
    vector<WINDOW*> wins;

    setlocale(LC_ALL, "");

    initscr();
    cbreak();
    keypad(stdscr, true);
    noecho();
    start_color();
    curs_set(0);
    erase();

    refresh();

    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    if (rows < 10 || cols < 10) throw runtime_error("Window too small");

    // Keep track of the data to present
    PandoData pd {pp, wins, rows, cols};

    // Setup the colors
    init_pair(1, COLOR_RED, COLOR_BLACK);
    init_pair(2, COLOR_BLUE, COLOR_BLACK);
    init_pair(3, COLOR_MAGENTA, COLOR_BLACK);

    // Build the windows
    WINDOW *w_header;
    w_header = newwin(3, 13, 1, 1);
    wins.push_back(w_header);
    box(w_header, 0, 0);
    wattron(w_header, COLOR_PAIR(1));
    mvwprintw(w_header, 1, 2, "PANDO TOP");
    wattroff(w_header, COLOR_PAIR(1));

    wrefresh(w_header);

    WINDOW *w_stats;
    w_stats = newwin(4, cols-14-7, 1, 14);
    wins.push_back(w_stats);
    box(w_stats, 0, 0);

    mvwprintw(w_stats, 1, 2, "Mesh size     : ");
    pd.set_mesh_pos(w_stats);
    mvwprintw(w_stats, 2, 2, "Total DB size : ");
    pd.set_db_pos(w_stats);

    pd.write_stats();

    WINDOW *w_up = newwin(3, 5, 1, cols-7);
    wins.push_back(w_up);
    box(w_up, 0, 0);
    wmove(w_up, 1, 2);
    pd.set_up_pos(w_up);

    pd.write_up();

    WINDOW *w_hl = newwin(1, cols, 5, 0);
    wins.push_back(w_hl);
    for (int c = 0; c < cols; ++c)
        wprintw(w_hl, "▄");
    wrefresh(w_hl);

    for (; !*g_shutdown; this_thread::sleep_for(chrono::milliseconds(10))) {
        int cur_rows, cur_cols;
        getmaxyx(stdscr, cur_rows, cur_cols);

        if (cur_rows != rows || cur_cols != cols) {
            // just reset the full top
            break;
        }

        pd.update();
    }

    for (auto revit = wins.rbegin(); revit != wins.rend(); ++revit)
        delwin(*revit);
    endwin();
}

int main_(int argc, char **argv) {
    if (argc < 2) {
        cerr << "Usage: pando_top mesh-addr\n"
            "\n"
            "Parameters:\n"
            "  mesh-addr : an address in the mesh to join\n"
            "\n"
            "Addresses are of the form: IPv4-string,ID\n"
            "  IPv4-string : a period separated IP address, e.g., 1.2.3.4\n"
            "  ID : a small integer between 0 and 199\n"
            ;
        return 1;
    }

    elga::ZMQAddress mesh_addr = get_zmq_addr(argv[1]);

    PandoTop pp { };
    pp.add_neighbor(mesh_addr.serialize(), false);
    pp.request_mesh(mesh_addr.serialize());

    volatile sig_atomic_t shutdown = false;
    g_shutdown = &shutdown;
    set_handler();

    while (!shutdown)
        top(pp);

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

