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

#include "build_options.hpp"
#include "cxxopts.hpp"
#include "garner.hpp"
#include "utils.hpp"

static constexpr unsigned NUM_ROUNDS = 3;
static constexpr uint64_t ROUND_SECS = 5;

static constexpr size_t KEY_LEN = 10;
static constexpr size_t VAL_LEN = 40;

static size_t TEST_DEGREE = 256;
static unsigned NUM_THREADS = 16;
static size_t NUM_OPS_WARMUP = 50000;
static size_t MAX_OPS_PER_TXN = 10;
static unsigned SCAN_PERCENTAGE = 25;
// TODO: control scan range key space

struct TxnStats {
    size_t num_txns = 0;
    size_t num_committed = 0;
    double exec_time = 0.;
    double lock_time = 0.;
    double validate_time = 0.;
    double commit_time = 0.;
};

static void client_thread_func(std::stop_token stop_token,
                               [[maybe_unused]] unsigned tidx,
                               garner::Garner* gn,
                               const std::vector<std::string>* warmup_keys,
                               TxnStats* stats, std::latch* init_barrier) {
    *stats = {0, 0, 0., 0., 0., 0.};

    std::random_device rd;
    std::mt19937 gen(rd());

    std::string val = gen_rand_string(gen, VAL_LEN);

    std::uniform_int_distribution<size_t> rand_idx(0, warmup_keys->size() - 1);
    std::uniform_int_distribution<unsigned> rand_is_scan(1, 100);

    // in non-scan transactions, 5% are write operations
    std::uniform_int_distribution<unsigned> rand_op_type(1, 20);

    auto GenRandomReq = [&](bool is_scan_txn) -> GarnerReq {
        // randomly pick an op type
        GarnerOp op = is_scan_txn ? SCAN : (rand_op_type(gen) == 1) ? PUT : GET;

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

        std::chrono::time_point<std::chrono::high_resolution_clock> start_tp;
        if constexpr (build_options.txn_stat)
            start_tp = std::chrono::high_resolution_clock::now();

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

        bool committed;
        if constexpr (!build_options.txn_stat)
            committed = gn->FinishTxn(txn);
        else {
            auto end_tp = std::chrono::high_resolution_clock::now();

            garner::TxnStats txn_stats;
            committed = gn->FinishTxn(txn, nullptr, nullptr, &txn_stats);

            // only record the time committed txns take since they don't always
            // go through all the phases
            if (committed) {
                stats->exec_time +=
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end_tp - start_tp)
                        .count();
                stats->lock_time += txn_stats.lock_time;
                stats->validate_time += txn_stats.validate_time;
                stats->commit_time += txn_stats.commit_time;
            }
        }

        ++stats->num_txns;
        if (committed) ++stats->num_committed;
    }
}

static void simple_benchmark_round(garner::TxnProtocol protocol) {
    auto* gn = garner::Garner::Open(TEST_DEGREE, protocol);

    std::cout << " Degree=" << TEST_DEGREE << " #threads=" << NUM_THREADS
              << " length=" << ROUND_SECS << "s"
              << " scan=" << SCAN_PERCENTAGE << "%" << std::endl;

    // garner::BPTreeStats stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

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

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Running multi-threaded transaction workload..." << std::endl;
    std::vector<TxnStats> thread_txn_stats(NUM_THREADS, {0, 0, 0., 0., 0., 0.});
    {
        std::vector<std::jthread> threads;
        std::latch init_barrier(NUM_THREADS);

        for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
            threads.push_back(
                std::jthread(client_thread_func, tidx, gn, &warmup_keys,
                             &thread_txn_stats[tidx], &init_barrier));
        }

        std::this_thread::sleep_for(std::chrono::seconds(ROUND_SECS));
        // destructor of jthread calls request_stop() and join()
    }

    size_t total_num_txns = 0, total_num_committed = 0;
    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
        total_num_txns += thread_txn_stats[tidx].num_txns;
        total_num_committed += thread_txn_stats[tidx].num_committed;
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

    if constexpr (build_options.txn_stat) {
        double total_exec_time = 0, total_lock_time = 0,
               total_validate_time = 0, total_commit_time = 0;
        for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
            total_exec_time += thread_txn_stats[tidx].exec_time;
            total_lock_time += thread_txn_stats[tidx].lock_time;
            total_validate_time += thread_txn_stats[tidx].validate_time;
            total_commit_time += thread_txn_stats[tidx].commit_time;
        }
        std::cout << "  Latency breakdown: " << std::endl;
        std::cout << "    Exec time:     " << std::fixed << std::setw(10)
                  << std::setprecision(4)
                  << total_exec_time / total_num_committed << " μs"
                  << std::endl;
        std::cout << "    Lock time:     " << std::fixed << std::setw(10)
                  << std::setprecision(4)
                  << total_lock_time / total_num_committed << " μs"
                  << std::endl;
        std::cout << "    Validate time: " << std::fixed << std::setw(10)
                  << std::setprecision(4)
                  << total_validate_time / total_num_committed << " μs"
                  << std::endl;
        std::cout << "    Commit time:   " << std::fixed << std::setw(10)
                  << std::setprecision(4)
                  << total_commit_time / total_num_committed << " μs"
                  << std::endl;
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Simple benchmarking round finished!" << std::endl;
    delete gn;
}

int main(int argc, char* argv[]) {
    bool help;
    std::string protocol_str;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "d,degree", "B+-tree degree",
        cxxopts::value<size_t>(TEST_DEGREE)->default_value("256"))(
        "w,warmup_ops", "number of warmup ops",
        cxxopts::value<size_t>(NUM_OPS_WARMUP)->default_value("50000"))(
        "p,protocol", "concurency control protocol",
        cxxopts::value<std::string>(protocol_str)->default_value("silo"))(
        "t,threads", "number of threads",
        cxxopts::value<unsigned>(NUM_THREADS)->default_value("16"))(
        "m,max_ops_txn", "max number of ops per transaction",
        cxxopts::value<size_t>(MAX_OPS_PER_TXN)->default_value("10"))(
        "c,scan_percent", "percentage of scan operations",
        cxxopts::value<unsigned>(SCAN_PERCENTAGE)->default_value("25"));
    auto result = cmd_args.parse(argc, argv);

    std::set<std::string> valid_protocols{"none", "silo", "silo_hv", "silo_nr"};

    if (help) {
        printf("%s", cmd_args.help().c_str());
        std::cout << std::endl << "Valid concurrency control protocols:  ";
        for (auto&& p : valid_protocols) std::cout << p << "  ";
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
    else if (protocol_str == "silo_nr")
        protocol = garner::PROTOCOL_SILO_NR;
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
