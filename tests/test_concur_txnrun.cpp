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
#include <queue>
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
static constexpr size_t NUM_OPS_WARMUP = 1000;

// using a very small key space to force many conflicts
// should see a rather high abort rate
static constexpr size_t KEY_LEN = 2;

static unsigned NUM_ROUNDS = 1;
static unsigned NUM_THREADS = 8;
static size_t NUM_OPS_PER_THREAD = 12000;
static size_t MAX_OPS_PER_TXN = 30;

static void client_thread_func(unsigned tidx, garner::Garner* gn,
                               uint64_t pre_putval,
                               const std::vector<std::string>* pre_putvec,
                               bool static_mode, std::vector<GarnerReq>* reqs,
                               std::atomic<uint64_t>* ser_counter,
                               std::latch* init_barrier) {
    reqs->clear();
    reqs->reserve(NUM_OPS_PER_THREAD);

    std::random_device rd;
    std::mt19937 gen(rd());

    uint64_t putval = pre_putval;
    std::vector<std::string> putvec(*pre_putvec);

    std::uniform_int_distribution<unsigned> rand_op_type(1, 3);
    std::uniform_int_distribution<unsigned> rand_get_source(1, 2);
    std::uniform_int_distribution<size_t> rand_idx(
        0, NUM_OPS_PER_THREAD + NUM_OPS_WARMUP - 1);

    auto GenRandomReq = [&](bool scan_only) -> GarnerReq {
        // randomly pick an op type
        unsigned op_choice = scan_only ? 3 : rand_op_type(gen);
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
            std::string key;
            if (static_mode) {
                assert(putvec.size() > 0);
                key = putvec[rand_idx(gen) % putvec.size()];
            } else
                key = gen_rand_string(gen, KEY_LEN);
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

    std::uniform_int_distribution<size_t> rand_txn_ops(1, MAX_OPS_PER_TXN);
    std::uniform_int_distribution<size_t> rand_txn_ops_scan(
        1, MAX_OPS_PER_TXN / 10);
    std::uniform_int_distribution<unsigned> rand_non_scan(0, 4);

    std::string get_buf = "";
    std::vector<std::tuple<std::string, std::string>> scan_result;
    uint64_t ser_order = 0;

    // sync all client threads here before doing work
    init_barrier->count_down();
    init_barrier->wait();

    size_t curr_ops = 0;
    while (curr_ops < NUM_OPS_PER_THREAD) {
        // generate scan only or not decision
        unsigned non_scan = rand_non_scan(gen);
        bool scan_txn = (non_scan == 0);

        // generate number of ops for this transaction
        size_t txn_ops = scan_txn ? rand_txn_ops_scan(gen) : rand_txn_ops(gen);
        if (curr_ops + txn_ops > NUM_OPS_PER_THREAD)
            txn_ops = NUM_OPS_PER_THREAD - curr_ops;

        auto* txn = gn->StartTxn();

        // generate random requests
        for (size_t j = 0; j < txn_ops; ++j) {
            GarnerReq req = GenRandomReq(scan_txn);

            if (req.op == GET) {
                bool found;
                gn->Get(req.key, get_buf, found, txn);
                req.value = get_buf;
                req.get_found = found;
                get_buf = "";
            } else if (req.op == PUT) {
                gn->Put(req.key, req.value, txn);
                putvec.push_back(req.key);
            } else {
                size_t nrecords;
                gn->Scan(req.key, req.rkey, scan_result, nrecords, txn);
                req.scan_result = scan_result;
                scan_result.clear();
            }

            reqs->push_back(std::move(req));
        }

        bool committed = gn->FinishTxn(txn, ser_counter, &ser_order);
        // save commit/abort result
        for (size_t j = 0; j < txn_ops; ++j) {
            reqs->at(curr_ops + j).committed = committed;
            reqs->at(curr_ops + j).ser_order = ser_order;
        }

        curr_ops += txn_ops;
    }
}

static void integrity_check(
    garner::Garner* gn, std::vector<std::vector<GarnerReq>*>* thread_reqs,
    const std::map<std::string, std::string>& warmup_map) {
    // gather possible final values of each key
    std::map<std::string, std::map<unsigned, std::string>> final_valid_vals;
    for (unsigned tidx = 0; tidx < thread_reqs->size(); ++tidx) {
        for (auto it = thread_reqs->at(tidx)->rbegin();
             it != thread_reqs->at(tidx)->rend(); ++it) {
            if (it->committed && it->op == PUT) {
                if (!final_valid_vals.contains(it->key))
                    final_valid_vals[it->key] =
                        std::map<unsigned, std::string>({{tidx, it->value}});
                else if (!final_valid_vals[it->key].contains(tidx))
                    final_valid_vals[it->key][tidx] = it->value;
            }
        }
    }
    for (auto&& [key, val] : warmup_map) {
        if (!final_valid_vals.contains(key))
            final_valid_vals[key] = std::map<unsigned, std::string>({{0, val}});
        else if (!final_valid_vals[key].contains(0))
            final_valid_vals[key][0] = val;
    }

    std::vector<std::tuple<std::string, std::string>> scan_result;
    std::string min_key(KEY_LEN, alphanum[0]);
    std::string max_key(KEY_LEN, alphanum[sizeof(alphanum) - 2]);
    size_t nrecords;
    bool scan_committed = gn->Scan(min_key, max_key, scan_result, nrecords);
    if (!scan_committed)
        throw FuzzTestException("Scan for integrity check aborted");

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

static void serializability_check(
    [[maybe_unused]] garner::Garner* gn,
    std::vector<std::vector<GarnerReq>*>* thread_reqs,
    const std::map<std::string, std::string>& warmup_map) {
    // use ser_order as the equivalent order and check against the result of
    // execution with that order
    //
    // The current implementation is NOT phantom-protected (so is repeatable
    // read instead of serializable). It is allowed that a Get returns
    // not_found when it should return found, or a Scan returns results fewer
    // than what should be scanned.
    unsigned nthreads = thread_reqs->size();
    size_t total_nreqs = 0;
    for (auto it = thread_reqs->begin(); it != thread_reqs->end(); ++it)
        total_nreqs += (*it)->size();

    std::map<std::string, std::string> refmap(warmup_map);

    auto CheckedPut = [&](std::string key, std::string val) {
        // std::cout << "Put " << key << " " << val << std::endl;
        refmap[key] = val;
    };

    auto CheckedGet = [&](const std::string& key, const std::string& val,
                          const bool& found) {
        // std::cout << "Get " << key;
        // if (found)
        //     std::cout << " found " << val << std::endl;
        // else
        //     std::cout << " not found" << std::endl;
        std::string refval = "null";
        bool reffound = refmap.contains(key);
        if (reffound) refval = refmap[key];
        if (reffound != found) {
            if (found && !reffound) {
                throw FuzzTestException("Get mismatch: key=" + key +
                                        " found=" + (found ? "T" : "F") +
                                        " reffound=" + (found ? "T" : "F"));
            }
        } else if (found && reffound && refval != val) {
            throw FuzzTestException("Get mismatch: key=" + key + " val=" + val +
                                    " refval=" + refval);
        }
    };

    auto CheckedScan =
        [&](const std::string& lkey, const std::string& rkey,
            const std::vector<std::tuple<std::string, std::string>>& results,
            const size_t& nrecords) {
            // std::cout << "Scan " << lkey << "-" << rkey;
            // std::cout << " got " << nrecords << std::endl;
            std::vector<std::tuple<std::string, std::string>> refresults;
            size_t refnrecords = 0;
            for (auto&& it = refmap.lower_bound(lkey);
                 it != refmap.upper_bound(rkey); ++it) {
                refresults.push_back(std::make_tuple(it->first, it->second));
                refnrecords++;
            }
            if (refnrecords != nrecords) {
                if (nrecords > refnrecords) {
                    throw FuzzTestException(
                        "Scan mismatch: lkey=" + lkey + " rkey=" + rkey +
                        " nrecords=" + std::to_string(nrecords) +
                        " refnrecords=" + std::to_string(refnrecords));
                }
            }
            for (auto&& [key, val] : results) {
                bool checked = false;
                for (auto&& [refkey, refval] : refresults) {
                    if (key == refkey) {
                        if (refval != val) {
                            throw FuzzTestException(
                                "Scan mismatch: lkey=" + lkey +
                                " rkey=" + rkey + " key=" + key +
                                " val=" + val + " refval=" + refval);
                        }
                        checked = true;
                        break;
                    }
                }
                if (!checked) {
                    throw FuzzTestException(
                        "Scan key shouldn't exist: lkey=" + lkey +
                        " rkey=" + rkey + " key=" + key);
                }
            }
        };

    size_t nreqs = 0, ncommitted = 0;
    std::vector<size_t> thread_idxs(nthreads, 0);
    std::vector<size_t> thread_nreqs(nthreads);
    for (unsigned t = 0; t < nthreads; ++t)
        thread_nreqs[t] = thread_reqs->at(t)->size();

    auto SerOrderGreater = [](const std::pair<unsigned, uint64_t>& pa,
                              const std::pair<unsigned, uint64_t>& pb) {
        return pa.second > pb.second;
    };
    std::priority_queue<std::pair<unsigned, uint64_t>,
                        std::vector<std::pair<unsigned, uint64_t>>,
                        decltype(SerOrderGreater)>
        minheap(SerOrderGreater);
    for (unsigned t = 0; t < nthreads; ++t) {
        while (thread_idxs[t] < thread_nreqs[t] &&
               !thread_reqs->at(t)->at(thread_idxs[t]).committed) {
            thread_idxs[t]++;
            nreqs++;
        }
        if (thread_idxs[t] < thread_nreqs[t]) {
            minheap.push(std::make_pair(
                t, thread_reqs->at(t)->at(thread_idxs[t]).ser_order));
        }
    }

    while (nreqs < total_nreqs) {
        assert(minheap.size() > 0);

        // find the head-of-line request with smallest ser_order across threads
        unsigned t;
        uint64_t ser_order;
        std::tie(t, ser_order) = minheap.top();
        minheap.pop();

        // apply the operations in that transation of that ser_order
        while (thread_idxs[t] < thread_nreqs[t]) {
            GarnerReq* req = &thread_reqs->at(t)->at(thread_idxs[t]);
            if (!req->committed || req->ser_order != ser_order) break;

            if (req->op == GET)
                CheckedGet(req->key, req->value, req->get_found);
            else if (req->op == PUT)
                CheckedPut(req->key, req->value);
            else
                CheckedScan(req->key, req->rkey, req->scan_result,
                            req->scan_result.size());

            thread_idxs[t]++;
            nreqs++;
            ncommitted++;
        }

        // push the next committed request of that thread into minheap
        while (thread_idxs[t] < thread_nreqs[t] &&
               !thread_reqs->at(t)->at(thread_idxs[t]).committed) {
            thread_idxs[t]++;
            nreqs++;
        }
        if (thread_idxs[t] < thread_nreqs[t]) {
            minheap.push(std::make_pair(
                t, thread_reqs->at(t)->at(thread_idxs[t]).ser_order));
        }
    }

    assert(minheap.empty());
    assert(nreqs >= ncommitted);
    size_t naborted = nreqs - ncommitted;
    double abort_rate =
        static_cast<double>(naborted) / static_cast<double>(nreqs);
    std::cout << "  Abort rate: " << naborted << " / " << nreqs << " ("
              << std::fixed << std::setw(4) << std::setprecision(1)
              << abort_rate * 100 << "%)" << std::endl;
}

static void concurrency_test_round(garner::TxnProtocol protocol,
                                   bool static_mode) {
    auto* gn = garner::Garner::Open(TEST_DEGREE, protocol);

    std::cout << " Degree=" << TEST_DEGREE << " #threads=" << NUM_THREADS
              << " #ops/thread=" << NUM_OPS_PER_THREAD
              << " static=" << (static_mode ? "yes" : "no") << std::endl;

    std::atomic<uint64_t> ser_counter{1};

    // garner::BPTreeStats stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Warming up B+-tree with some records..." << std::endl;
    std::random_device rd;
    std::mt19937 gen(rd());

    uint64_t pre_putval = 1000;  // monotonically increasing on each thread
    std::vector<std::string> pre_putvec;
    pre_putvec.reserve(NUM_OPS_WARMUP);
    std::map<std::string, std::string> warmup_map;

    for (size_t j = 0; j < NUM_OPS_WARMUP; ++j) {
        std::string key = gen_rand_string(gen, KEY_LEN);
        std::string val = std::to_string(0) + "-" + std::to_string(pre_putval);
        pre_putval++;
        gn->Put(key, val);
        pre_putvec.push_back(key);
        warmup_map[key] = val;
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // spawn multiple threads, each doing a sufficient number of requests,
    // and recording the list of requests (+ results) on each
    std::cout << " Running multi-threaded transaction workload..." << std::endl;
    std::vector<std::thread> threads;
    std::vector<std::vector<GarnerReq>*> thread_reqs;
    std::latch init_barrier(NUM_THREADS);

    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) {
        thread_reqs.push_back(new std::vector<GarnerReq>);
        threads.push_back(std::thread(
            client_thread_func, tidx, gn, pre_putval, &pre_putvec, static_mode,
            thread_reqs[tidx], &ser_counter, &init_barrier));
    }
    for (unsigned tidx = 0; tidx < NUM_THREADS; ++tidx) threads[tidx].join();

    // run an (incomplete) serializability check against thread results
    std::cout << " Doing basic integrity check..." << std::endl;
    integrity_check(gn, &thread_reqs, warmup_map);

    // run a serializability check against thread results
    std::cout << " Doing serializability check..." << std::endl;
    serializability_check(gn, &thread_reqs, warmup_map);

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Concurrent transaction tests passed!" << std::endl;
    delete gn;
    for (auto* tr : thread_reqs) delete tr;
}

int main(int argc, char* argv[]) {
    bool help, static_mode;
    std::string protocol_str;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "r,rounds", "number of rounds",
        cxxopts::value<unsigned>(NUM_ROUNDS)->default_value("1"))(
        "p,protocol", "concurency control protocol",
        cxxopts::value<std::string>(protocol_str)->default_value("silo"))(
        "t,threads", "number of threads",
        cxxopts::value<unsigned>(NUM_THREADS)->default_value("8"))(
        "o,ops", "number of ops per thread per round",
        cxxopts::value<size_t>(NUM_OPS_PER_THREAD)->default_value("12000"))(
        "m,max_ops_txn", "max number of ops per transaction",
        cxxopts::value<size_t>(MAX_OPS_PER_TXN)->default_value("30"))(
        "s,static", "if set, disallow on-the-fly insertions",
        cxxopts::value<bool>(static_mode)->default_value("false"));
    auto result = cmd_args.parse(argc, argv);

    std::set<std::string> valid_protocols{"none", "silo", "silo_hv"};

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
    else {
        std::cerr << "Error: unrecognized concurrency control protocol: "
                  << protocol_str << std::endl;
        return 1;
    }

    if (NUM_OPS_PER_THREAD < 10 ||
        (NUM_OPS_PER_THREAD - NUM_OPS_PER_THREAD / 10) < MAX_OPS_PER_TXN) {
        std::cerr << "Error: number of ops per thread per round too small "
                  << NUM_OPS_PER_THREAD << std::endl;
        return 1;
    } else if (MAX_OPS_PER_TXN < 10) {
        std::cerr << "Error: max number of ops per transaction too small "
                  << MAX_OPS_PER_TXN << std::endl;
        return 1;
    }

    std::srand(std::time(NULL));

    for (unsigned round = 0; round < NUM_ROUNDS; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        concurrency_test_round(protocol, static_mode);
    }

    return 0;
}
