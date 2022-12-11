#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <latch>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cxxopts.hpp"
#include "garner.hpp"
#include "utils.hpp"

static constexpr size_t TEST_DEGREE = 128;
static constexpr size_t NUM_OPS_WARMUP = 20000;
static constexpr size_t KEY_LEN = 10;
static constexpr size_t VAL_LEN = 40;

static unsigned NUM_THREADS = 8;
static size_t MAX_OPS_PER_TXN = 10;
static unsigned SCAN_PERCENTAGE = 10;

static unsigned NUM_ROUNDS = 3;
static uint64_t ROUND_SECS = 5;

static void client_thread_func(std::stop_token stop_token,
                               [[maybe_unused]] unsigned tidx,
                               garner::Garner* gn,
                               const std::vector<std::string>* warmup_keys,
                               size_t* num_txns, size_t* num_committed,
                               std::latch* init_barrier) {
    *num_txns = 0;
    *num_committed = 0;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::string val = gen_rand_string(gen, VAL_LEN);

    std::uniform_int_distribution<unsigned> rand_is_scan(1, 100);
    std::uniform_int_distribution<unsigned> rand_op_type(1, 2);
    std::uniform_int_distribution<size_t> rand_idx(0, warmup_keys->size() - 1);

    auto GenRandomReq = [&](bool is_scan_txn) -> GarnerReq {
        // randomly pick an op type
        GarnerOp op = is_scan_txn ? SCAN : (rand_op_type(gen) == 1) ? GET : PUT;

        if (op == GET) {
            std::string key = warmup_keys->at(rand_idx(gen));
            return GarnerReq(GET, std::move(key), "", "");

        } else if (op == PUT) {
            std::string key = warmup_keys->at(rand_idx(gen));
            return GarnerReq(PUT, std::move(key), "", val);

        } else {
            std::string lkey = gen_rand_string(gen, KEY_LEN);
            std::string rkey;
            do {
                rkey = gen_rand_string(gen, KEY_LEN);
            } while (rkey < lkey);
            return GarnerReq(SCAN, std::move(lkey), std::move(rkey), "");
        }
    };

    std::uniform_int_distribution<size_t> rand_txn_ops(1, MAX_OPS_PER_TXN);
    std::uniform_int_distribution<size_t> rand_txn_ops_scan(
        1, MAX_OPS_PER_TXN / 10);
    std::uniform_int_distribution<unsigned> rand_is_scan_txn(1, 100);

    std::string get_buf;
    bool get_found;
    std::vector<std::tuple<std::string, std::string>> scan_result;
    size_t scan_nrecords;

    // sync all client threads here before doing work
    init_barrier->count_down();
    init_barrier->wait();

    while (true) {
        // if stop requested, break
        if (stop_token.stop_requested()) break;

        // generate scan only or not decision
        bool scan_txn = (rand_is_scan_txn(gen) < SCAN_PERCENTAGE);

        // generate number of ops for this transaction
        size_t txn_ops = scan_txn ? rand_txn_ops_scan(gen) : rand_txn_ops(gen);

        auto* txn = gn->StartTxn();

        // generate random requests
        for (size_t j = 0; j < txn_ops; ++j) {
            GarnerReq req = GenRandomReq(scan_txn);

            if (req.op == GET) {
                gn->Get(req.key, get_buf, get_found, txn);
            } else if (req.op == PUT) {
                gn->Put(req.key, req.value, txn);
            } else {
                gn->Scan(req.key, req.rkey, scan_result, scan_nrecords, txn);
                scan_result.clear();
            }
        }

        bool committed = gn->FinishTxn(txn);
        ++(*num_txns);
        if (committed) ++(*num_committed);
    }
}

static void simple_benchmark_round(garner::TxnProtocol protocol) {
    auto* gn = garner::Garner::Open(TEST_DEGREE, protocol);

    std::cout << " Degree=" << TEST_DEGREE << " #threads=" << NUM_THREADS
              << " length=" << ROUND_SECS << "s" << std::endl;

    // gn->PrintStats(true);

    std::cout << " Warming up B+-tree with " << NUM_OPS_WARMUP << " records..."
              << std::endl;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::string val = gen_rand_string(gen, VAL_LEN);

    std::vector<std::string> warmup_keys;
    warmup_keys.reserve(NUM_OPS_WARMUP);

    for (size_t j = 0; j < NUM_OPS_WARMUP; ++j) {
        std::string key;
        do {
            key = gen_rand_string(gen, KEY_LEN);
        } while (std::find(warmup_keys.begin(), warmup_keys.end(), key) !=
                 warmup_keys.end());
        gn->Put(key, val);
        warmup_keys.push_back(key);
    }

    // gn->PrintStats(true);

    std::cout << " Running multi-threaded transaction workload..." << std::endl;
    std::vector<size_t> thread_num_txns(NUM_THREADS, 0);
    std::vector<size_t> thread_num_committed(NUM_THREADS, 0);
    {
        std::vector<std::jthread> threads;
        std::latch init_barrier(NUM_THREADS);

        for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
            threads.push_back(std::jthread(client_thread_func, tidx, gn,
                                           &warmup_keys, &thread_num_txns[tidx],
                                           &thread_num_committed[tidx],
                                           &init_barrier));
        }

        std::this_thread::sleep_for(std::chrono::seconds(ROUND_SECS));
        // destructor of jthread calls request_stop() and join()
    }

    size_t total_num_txns = 0, total_num_committed = 0;
    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
        total_num_txns += thread_num_txns[tidx];
        total_num_committed += thread_num_committed[tidx];
    }
    size_t total_num_aborted = total_num_txns - total_num_committed;
    double abort_rate = static_cast<double>(total_num_aborted) /
                        static_cast<double>(total_num_txns);
    std::cout << "  Abort rate: " << total_num_aborted << " / "
              << total_num_txns << " (" << std::fixed << std::setw(4)
              << std::setprecision(1) << abort_rate * 100 << "%)" << std::endl;

    double throughput = static_cast<double>(total_num_committed) / 5.0;
    std::cout << "  Throughput: " << std::fixed << std::setw(0)
              << std::setprecision(2) << throughput << " txns/sec" << std::endl;

    // gn->PrintStats(true);

    std::cout << " Simple benchmarking round finished!" << std::endl;
    delete gn;
}

int main(int argc, char* argv[]) {
    bool help;
    std::string protocol_str;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "r,rounds", "number of rounds",
        cxxopts::value<unsigned>(NUM_ROUNDS)->default_value("3"))(
        "s,secs", "number of seconds per round",
        cxxopts::value<uint64_t>(ROUND_SECS)->default_value("5"))(
        "p,protocol", "concurency control protocol",
        cxxopts::value<std::string>(protocol_str)->default_value("silo"))(
        "t,threads", "number of threads",
        cxxopts::value<unsigned>(NUM_THREADS)->default_value("8"))(
        "m,max_ops_txn", "max number of ops per transaction",
        cxxopts::value<size_t>(MAX_OPS_PER_TXN)->default_value("10"))(
        "c,scan_percent", "percentage of scan operations",
        cxxopts::value<unsigned>(SCAN_PERCENTAGE)->default_value("10"));
    auto result = cmd_args.parse(argc, argv);

    std::set<std::string> valid_protocols{"none", "silo", "silo_hv"};

    if (help) {
        printf("%s", cmd_args.help().c_str());
        std::cout << std::endl << "Valid concurrency control protocols: ";
        for (auto&& p : valid_protocols) std::cout << p << " ";
        std::cout << std::endl;
        return 0;
    }

    garner::TxnProtocol protocol;
    if (protocol_str == "none")
        protocol = garner::PROTOCOL_NONE;
    else if (protocol_str == "silo")
        protocol = garner::PROTOCOL_SILO;
    else if (protocol_str == "silo_hv")
        protocol = garner::PROTOCOL_SILO_HV;
    else {
        std::cerr << "Error: unrecognized concurrency control protocol: "
                  << protocol_str << std::endl;
        return 1;
    }

    if (MAX_OPS_PER_TXN < 10) {
        std::cerr << "Error: max number of ops per transaction too small "
                  << MAX_OPS_PER_TXN << std::endl;
        return 1;
    }

    std::srand(std::time(NULL));

    for (unsigned round = 0; round < NUM_ROUNDS; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        simple_benchmark_round(protocol);
    }

    return 0;
}
