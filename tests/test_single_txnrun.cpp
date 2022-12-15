#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "cxxopts.hpp"
#include "garner.hpp"
#include "utils.hpp"

static constexpr size_t TEST_DEGREE = 8;
static constexpr size_t KEY_LEN = 8;
static constexpr size_t VAL_LEN = 10;

static unsigned NUM_ROUNDS = 20;
static size_t NUM_OPS = 1000;
static size_t MAX_OPS_PER_TXN = 20;

static void single_test_round(garner::TxnProtocol protocol) {
    auto* gn = garner::Garner::Open(TEST_DEGREE, protocol);

    std::random_device rd;
    std::mt19937 gen(rd());

    uint64_t putval = 1000;  // monotonically increasing on each thread
    std::vector<std::string> putvec;
    putvec.reserve(NUM_OPS);

    std::uniform_int_distribution<unsigned> rand_op_type(1, 3);
    std::uniform_int_distribution<unsigned> rand_get_source(1, 2);
    std::uniform_int_distribution<size_t> rand_idx(0, NUM_OPS - 1);

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
            std::string val = std::to_string(putval);
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

    std::map<std::string, std::string> refmap;
    std::vector<std::string> refvec;

    auto CheckedPut = [&](std::string key, std::string val,
                          garner::TxnCxt<std::string, std::string>* txn) {
        // std::cout << "Put " << key << " " << val << std::endl;
        gn->Put(key, val, txn);
        if (!refmap.contains(key)) refvec.push_back(key);
        refmap[key] = val;
    };

    auto CheckedGet = [&](const std::string& key,
                          garner::TxnCxt<std::string, std::string>* txn) {
        std::string val = "", refval = "null";
        bool found = false, reffound = false;
        // std::cout << "Get " << key;
        gn->Get(key, val, found, txn);
        // if (found)
        //     std::cout << " found " << val << std::endl;
        // else
        //     std::cout << " not found" << std::endl;
        reffound = refmap.contains(key);
        if (reffound) refval = refmap[key];
        if (reffound != found) {
            throw FuzzTestException("Get mismatch: key=" + key +
                                    " found=" + (found ? "T" : "F") +
                                    " reffound=" + (found ? "T" : "F"));
        } else if (found && reffound && refval != val) {
            throw FuzzTestException("Get mismatch: key=" + key + " val=" + val +
                                    " refval=" + refval);
        }
    };

    auto CheckedScan = [&](const std::string& lkey, const std::string& rkey,
                           garner::TxnCxt<std::string, std::string>* txn) {
        std::vector<std::tuple<std::string, std::string>> results, refresults;
        size_t nrecords = 0, refnrecords = 0;
        // std::cout << "Scan " << lkey << "-" << rkey;
        gn->Scan(lkey, rkey, results, nrecords, txn);
        // std::cout << " got " << nrecords << std::endl;
        for (auto&& it = refmap.lower_bound(lkey);
             it != refmap.upper_bound(rkey); ++it) {
            refresults.push_back(std::make_tuple(it->first, it->second));
            refnrecords++;
        }
        if (refnrecords != nrecords) {
            throw FuzzTestException(
                "Scan mismatch: lkey=" + lkey + " rkey=" + rkey +
                " nrecords=" + std::to_string(nrecords) +
                " refnrecords=" + std::to_string(refnrecords));
        } else {
            for (size_t i = 0; i < nrecords; ++i) {
                auto [key, val] = results[i];
                auto [refkey, refval] = refresults[i];
                if (refkey != key) {
                    throw FuzzTestException("Scan mismatch: lkey=" + lkey +
                                            " rkey=" + rkey + " key=" + key +
                                            " refkey=" + refkey);
                } else if (refval != val) {
                    throw FuzzTestException(
                        "Scan mismatch: lkey=" + lkey + " rkey=" + rkey +
                        " key=" + key + " val=" + val + " refval=" + refval);
                }
            }
        }
    };

    // garner::BPTreeStats stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // implicit single-op transactions without calling transaction interface
    size_t num_implicit = NUM_OPS / 10;
    for (size_t i = 0; i < num_implicit; ++i) {
        // generate a random request
        GarnerReq req = GenRandomReq();

        if (req.op == GET)
            CheckedGet(req.key, nullptr);
        else if (req.op == PUT)
            CheckedPut(req.key, req.value, nullptr);
        else
            CheckedScan(req.key, req.rkey, nullptr);
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // explicit transactions
    std::uniform_int_distribution<size_t> rand_txn_ops(1, MAX_OPS_PER_TXN);
    size_t curr_ops = num_implicit;
    while (curr_ops < NUM_OPS) {
        // generate number of ops for this transaction
        size_t txn_ops = rand_txn_ops(gen);
        if (curr_ops + txn_ops > NUM_OPS) txn_ops = NUM_OPS - curr_ops;

        auto* txn = gn->StartTxn();

        // generate random requests
        for (size_t j = 0; j < txn_ops; ++j) {
            GarnerReq req = GenRandomReq();

            if (req.op == GET)
                CheckedGet(req.key, txn);
            else if (req.op == PUT)
                CheckedPut(req.key, req.value, txn);
            else
                CheckedScan(req.key, req.rkey, txn);
        }

        bool committed = gn->FinishTxn(txn);
        if (!committed)
            throw FuzzTestException("transaction aborted with single thread");

        curr_ops += txn_ops;
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Single-thread transaction tests passed!" << std::endl;
    delete gn;
}

int main(int argc, char* argv[]) {
    bool help;
    std::string protocol_str;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "r,rounds", "number of rounds",
        cxxopts::value<unsigned>(NUM_ROUNDS)->default_value("20"))(
        "p,protocol", "concurency control protocol",
        cxxopts::value<std::string>(protocol_str)->default_value("silo"))(
        "o,ops", "number of ops per round",
        cxxopts::value<size_t>(NUM_OPS)->default_value("1000"))(
        "m,max_ops_txn", "max number of ops per transaction",
        cxxopts::value<size_t>(MAX_OPS_PER_TXN)->default_value("20"));
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

    if (NUM_OPS < 10 || (NUM_OPS - NUM_OPS / 10) < MAX_OPS_PER_TXN) {
        std::cerr << "Error: number of ops per round too small " << NUM_OPS
                  << std::endl;
        return 1;
    }

    std::srand(std::time(NULL));

    for (unsigned round = 0; round < NUM_ROUNDS; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        std::cout << " Degree=" << TEST_DEGREE << " Protocol=" << protocol_str
                  << " #ops=" << NUM_OPS << std::endl;
        single_test_round(protocol);
    }

    return 0;
}
