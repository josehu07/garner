#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <latch>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "cxxopts.hpp"
#include "garner.hpp"
#include "utils.hpp"

static constexpr size_t TEST_DEGREE = 6;
static constexpr size_t KEY_LEN = 2;

static unsigned NUM_ROUNDS = 5;
static unsigned NUM_THREADS = 8;
static size_t NUM_OPS_PER_THREAD = 5000;

static void client_thread_func(unsigned tidx, garner::Garner* gn,
                               std::vector<GarnerReq>* reqs,
                               std::latch* init_barrier) {
    reqs->clear();
    reqs->reserve(NUM_OPS_PER_THREAD);

    uint64_t putval = 1000;  // monotonically increasing on each thread
    std::vector<std::string> putvec;
    putvec.reserve(NUM_OPS_PER_THREAD);

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<unsigned> rand_op_type(1, 3);
    std::uniform_int_distribution<unsigned> rand_get_source(1, 2);
    std::uniform_int_distribution<size_t> rand_idx(0, NUM_OPS_PER_THREAD - 1);

    auto GenRandomReq = [&]() -> GarnerReq {
        // randomly pick an op type
        unsigned op_choice = rand_op_type(gen);
        GarnerOp op = (op_choice == 1) ? GET : (op_choice == 2) ? PUT : SCAN;

        if (op == GET) {
            // randomly pick should-found Get vs. unsure Get
            unsigned get_choice = rand_get_source(gen);
            std::string key;
            if (get_choice == 1 && putvec.size() > 0)
                key = putvec[rand_idx(gen) % putvec.size()];
            else {
                do {
                    key = gen_rand_string(gen, KEY_LEN);
                } while (std::find(putvec.begin(), putvec.end(), key) !=
                         putvec.end());
            }

            return GarnerReq(GET, key);

        } else if (op == PUT) {
            std::string key = gen_rand_string(gen, KEY_LEN);
            std::string val =
                std::to_string(tidx) + "-" + std::to_string(putval);
            putval++;

            return GarnerReq(PUT, std::move(key), std::move(val));

        } else {
            std::string lkey = gen_rand_string(gen, KEY_LEN);
            std::string rkey;
            do {
                rkey = gen_rand_string(gen, KEY_LEN);
            } while (rkey < lkey);

            return GarnerReq(SCAN, std::move(lkey), std::move(rkey), {});
        }
    };

    std::string get_buf = "";
    std::vector<std::tuple<std::string, std::string>> scan_result;

    // sync all client threads here before doing work
    init_barrier->count_down();
    init_barrier->wait();

    for (size_t i = 0; i < NUM_OPS_PER_THREAD; ++i) {
        // generate a random request
        GarnerReq req = GenRandomReq();

        if (req.op == GET) {
            bool found;
            gn->Get(req.key, get_buf, found);
            req.value = get_buf;
            req.get_found = found;
            get_buf = "";
        } else if (req.op == PUT) {
            gn->Put(req.key, req.value);
            putvec.push_back(req.key);
        } else {
            size_t nrecords;
            gn->Scan(req.key, req.rkey, scan_result, nrecords);
            req.scan_result = scan_result;
            scan_result.clear();
        }

        reqs->push_back(std::move(req));
    }
}

static void integrity_check(garner::Garner* gn,
                            std::vector<std::vector<GarnerReq>*> thread_reqs) {
    // this is NOT a comprehensive sequential consistency check, rather just
    // checking the final state's integrity, due to performance reasons

    std::map<std::string, std::map<unsigned, std::string>> final_valid_vals;
    for (unsigned tidx = 0; tidx < thread_reqs.size(); ++tidx) {
        for (auto it = thread_reqs[tidx]->rbegin();
             it != thread_reqs[tidx]->rend(); ++it) {
            if (it->op == PUT) {
                if (!final_valid_vals.contains(it->key))
                    final_valid_vals[it->key] =
                        std::map<unsigned, std::string>({{tidx, it->value}});
                else if (!final_valid_vals[it->key].contains(tidx))
                    final_valid_vals[it->key][tidx] = it->value;
            }
        }
    }

    std::vector<std::tuple<std::string, std::string>> scan_result;
    std::string min_key(KEY_LEN, alphanum[0]);
    std::string max_key(KEY_LEN, alphanum[sizeof(alphanum) - 2]);
    size_t nrecords;
    gn->Scan(min_key, max_key, scan_result, nrecords);
    if (scan_result.size() == 0)
        throw FuzzTestException("Scan returned 0 results");
    if (scan_result.size() != nrecords)
        throw FuzzTestException("Scan returned incorrect #results: nrecords=" +
                                std::to_string(nrecords) + " len(result)=" +
                                std::to_string(scan_result.size()));

    for (auto [key, val] : scan_result) {
        if (!final_valid_vals.contains(key)) {
            throw FuzzTestException("key " + key +
                                    " was never put by any thread");
        }

        auto dash_it = std::find(val.begin(), val.end(), '-');
        if (val.size() < 3 || dash_it == val.begin() || dash_it == val.end() ||
            dash_it == val.end() - 1) {
            throw FuzzTestException("value has invalid format: " + val);
        }

        unsigned tidx = std::stoul(val.substr(0, dash_it - val.begin()));
        if (!final_valid_vals[key].contains(tidx)) {
            throw FuzzTestException("key " + key + " was never put by thread " +
                                    std::to_string(tidx));
        }
        if (final_valid_vals[key][tidx] != val) {
            throw FuzzTestException("mismatch value for key " + key +
                                    ": val=" + val +
                                    " refval=" + final_valid_vals[key][tidx]);
        }
    }
}

static void concurrency_test_round() {
    auto* gn = garner::Garner::Open(TEST_DEGREE, garner::PROTOCOL_NONE);

    std::cout << " Degree=" << TEST_DEGREE << " #threads=" << NUM_THREADS
              << " #ops/thread=" << NUM_OPS_PER_THREAD << std::endl;

    // garner::BPTreeStats stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // spawn multiple threads, each doing a sufficient number of requests,
    // and recording the list of requests (+ results) on each
    std::cout << " Running multi-threaded B+-tree workload... " << std::endl;
    std::vector<std::thread> threads;
    std::vector<std::vector<GarnerReq>*> thread_reqs;
    std::latch init_barrier(NUM_THREADS);

    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
        thread_reqs.push_back(new std::vector<GarnerReq>);
        threads.push_back(std::thread(client_thread_func, tidx, gn,
                                      thread_reqs[tidx], &init_barrier));
    }
    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) threads[tidx].join();

    gn->GatherStats(false);
    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // run an (incomplete) integrity check against thread results
    std::cout << " Doing basic integrity check..." << std::endl;
    integrity_check(gn, std::move(thread_reqs));

    std::cout << " Concurrent BPTree tests passed!" << std::endl;
    delete gn;
    for (auto* tr : thread_reqs) delete tr;
}

int main(int argc, char* argv[]) {
    bool help;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "r,rounds", "number of rounds",
        cxxopts::value<unsigned>(NUM_ROUNDS)->default_value("5"))(
        "t,threads", "number of threads",
        cxxopts::value<unsigned>(NUM_THREADS)->default_value("8"))(
        "o,ops", "number of ops per thread per round",
        cxxopts::value<size_t>(NUM_OPS_PER_THREAD)->default_value("5000"));
    auto result = cmd_args.parse(argc, argv);

    if (help) {
        printf("%s", cmd_args.help().c_str());
        return 0;
    }

    std::srand(std::time(NULL));

    for (unsigned round = 0; round < NUM_ROUNDS; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        concurrency_test_round();
    }

    return 0;
}
