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
static constexpr size_t NUM_FOUND_GETS = 15;
static constexpr size_t NUM_NOTFOUND_GETS = 5;
static constexpr size_t NUM_SCANS = 10;

static unsigned NUM_ROUNDS = 100;

static void fuzz_test_round(bool do_puts) {
    auto* gn = garner::Garner::Open(TEST_DEGREE, garner::PROTOCOL_NONE);

    std::random_device rd;
    std::mt19937 gen(rd());

    size_t NUM_PUTS = 0;
    if (do_puts) {
        std::uniform_int_distribution<size_t> rand_nputs_small(1, 6 * 2);
        std::uniform_int_distribution<size_t> rand_nputs_medium(6 * 2 + 1,
                                                                6 * 6 * 2);
        std::uniform_int_distribution<size_t> rand_nputs_large(6 * 6 * 2 + 1,
                                                               6 * 6 * 6 * 3);
        unsigned choice = std::uniform_int_distribution<unsigned>(1, 3)(gen);
        NUM_PUTS = (choice == 1) ? rand_nputs_small(gen)
                                 : (choice == 2) ? rand_nputs_medium(gen)
                                                 : rand_nputs_large(gen);
    }

    std::cout << " Degree=" << TEST_DEGREE << " #puts=" << NUM_PUTS
              << std::endl;

    std::map<std::string, std::string> refmap;
    std::vector<std::string> refvec;

    auto CheckedPut = [&](std::string key, std::string val) {
        // std::cout << "Put " << key << " " << val << std::endl;
        gn->Put(key, val);
        if (!refmap.contains(key)) refvec.push_back(key);
        refmap[key] = val;
    };

    auto CheckedGet = [&](const std::string& key) {
        std::string val = "", refval = "null";
        bool found = false, reffound = false;
        // std::cout << "Get " << key;
        gn->Get(key, val, found);
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

    auto CheckedScan = [&](const std::string& lkey, const std::string& rkey) {
        std::vector<std::tuple<std::string, std::string>> results, refresults;
        size_t nrecords = 0, refnrecords = 0;
        // std::cout << "Scan " << lkey << "-" << rkey;
        gn->Scan(lkey, rkey, results, nrecords);
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

    // putting random records
    if (do_puts) {
        std::cout << " Testing random Puts..." << std::endl;
        for (size_t i = 0; i < NUM_PUTS; ++i) {
            std::string key = gen_rand_string(gen, KEY_LEN);
            std::string val = gen_rand_string(gen, VAL_LEN);
            CheckedPut(std::move(key), std::move(val));
        }
    }

    gn->GatherStats(false);
    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // getting keys that should be found
    if (do_puts) {
        std::cout << " Testing found Gets..." << std::endl;
        std::uniform_int_distribution<size_t> rand_idx(0, refvec.size() - 1);
        for (size_t i = 0; i < NUM_FOUND_GETS; ++i) {
            std::string key = refvec[rand_idx(gen)];
            CheckedGet(key);
        }
    }

    // getting keys that should not be found
    std::cout << " Testing not-found Gets..." << std::endl;
    for (size_t i = 0; i < NUM_NOTFOUND_GETS; ++i) {
        std::string key;
        do {
            key = gen_rand_string(gen, KEY_LEN);
        } while (refmap.contains(key));
        CheckedGet(key);
    }

    // changing values in-place
    if (do_puts) {
        std::cout << " Testing in-place Puts..." << std::endl;
        std::uniform_int_distribution<size_t> rand_idx(0, refvec.size() - 1);
        for (size_t i = 0; i < NUM_PUTS; ++i) {
            std::string key = refvec[rand_idx(gen)];
            std::string val = gen_rand_string(gen, VAL_LEN);
            CheckedPut(std::move(key), std::move(val));
        }
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    // scanning random ranges
    std::cout << " Testing random Scans..." << std::endl;
    for (size_t i = 0; i < NUM_SCANS; ++i) {
        std::string lkey = gen_rand_string(gen, KEY_LEN);
        std::string rkey;
        do {
            rkey = gen_rand_string(gen, KEY_LEN);
        } while (rkey < lkey);
        CheckedScan(lkey, rkey);
    }

    // stats = gn->GatherStats(true);
    // std::cout << stats << std::endl;

    std::cout << " Single-thread BPTree tests passed!" << std::endl;
    delete gn;
}

int main(int argc, char* argv[]) {
    bool help;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "r,rounds", "number of rounds",
        cxxopts::value<unsigned>(NUM_ROUNDS)->default_value("100"));
    auto result = cmd_args.parse(argc, argv);

    if (help) {
        printf("%s", cmd_args.help().c_str());
        return 0;
    }

    std::srand(std::time(NULL));

    for (unsigned round = 0; round < NUM_ROUNDS; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        fuzz_test_round(round != 0);
    }

    return 0;
}
