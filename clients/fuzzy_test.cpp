#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "cxxopts.hpp"
#include "garner.hpp"

class FuzzyTestException : public std::exception {
    std::string what_msg;

   public:
    FuzzyTestException(std::string&& what_msg) : what_msg(what_msg) {}
    ~FuzzyTestException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

static void fuzzy_test_round(const std::string& filename, bool do_puts) {
    constexpr size_t TEST_DEGREE = 6;
    constexpr uint64_t MAX_KEY = 1200;
    constexpr size_t NUM_FOUND_GETS = 15;
    constexpr size_t NUM_NOTFOUND_GETS = 5;
    constexpr size_t NUM_TINY_SCANS = 10;
    constexpr size_t NUM_NORMAL_SCANS = 7;
    constexpr size_t NUM_HUGE_SCANS = 3;

    std::filesystem::remove(filename);
    auto* garner = garner::Garner::Open(filename, TEST_DEGREE);

    std::srand(std::time(NULL));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> randkey(0, MAX_KEY);

    size_t NUM_PUTS = 0;
    if (do_puts) {
        std::uniform_int_distribution<size_t> randnputs(1, 6 * 6 * 6 * 3);
        NUM_PUTS = randnputs(gen);
    }

    std::map<uint64_t, uint64_t> refmap;
    std::vector<uint64_t> refvec;

    auto CheckedPut = [&](uint64_t key, uint64_t val) {
        // std::cout << "Put " << key << " " << val << std::endl;
        garner->Put(key, val);
        refmap[key] = val;
        refvec.push_back(key);
    };
    auto CheckedGet = [&](uint64_t key) {
        uint64_t val = 0, refval = 1;
        bool found = false, reffound = false;
        // std::cout << "Get " << key;
        found = garner->Get(key, val);
        // if (found)
        //     std::cout << " found " << val << std::endl;
        // else
        //     std::cout << " not found" << std::endl;
        reffound = refmap.contains(key);
        if (reffound) refval = refmap[key];
        if (reffound != found) {
            throw FuzzyTestException(
                "Get mismatch: key=" + std::to_string(key) + " found=" +
                (found ? "T" : "F") + " reffound=" + (found ? "T" : "F"));
        } else if (found && reffound && refval != val) {
            throw FuzzyTestException(
                "Get mismatch: key=" + std::to_string(key) + " val=" +
                std::to_string(val) + " refval=" + std::to_string(refval));
        }
    };
    auto CheckedScan = [&](uint64_t lkey, uint64_t rkey) {
        std::vector<std::tuple<uint64_t, uint64_t>> results, refresults;
        size_t nrecords = 0, refnrecords = 0;
        // std::cout << "Scan " << lkey << "-" << rkey;
        nrecords = garner->Scan(lkey, rkey, results);
        // std::cout << " got " << nrecords << std::endl;
        for (auto&& it = refmap.lower_bound(lkey);
             it != refmap.upper_bound(rkey); ++it) {
            refresults.push_back(std::make_tuple(it->first, it->second));
            refnrecords++;
        }
        if (refnrecords != nrecords) {
            throw FuzzyTestException(
                "Scan mismatch: lkey=" + std::to_string(lkey) + " rkey=" +
                std::to_string(rkey) + " nrecords=" + std::to_string(nrecords) +
                " refnrecords=" + std::to_string(refnrecords));
        } else {
            for (size_t i = 0; i < nrecords; ++i) {
                auto [key, val] = results[i];
                auto [refkey, refval] = refresults[i];
                if (refkey != key) {
                    throw FuzzyTestException(
                        "Scan mismatch: lkey=" + std::to_string(lkey) +
                        " rkey=" + std::to_string(rkey) +
                        " key=" + std::to_string(key) +
                        " refkey=" + std::to_string(refkey));
                } else if (refval != val) {
                    throw FuzzyTestException(
                        "Scan mismatch: lkey=" + std::to_string(lkey) +
                        " rkey=" + std::to_string(rkey) +
                        " val=" + std::to_string(val) +
                        " refval=" + std::to_string(refval));
                }
            }
        }
    };

    // garner->PrintStats(true);

    // putting a bunch of random records
    std::cout << " Testing random Puts..." << std::endl;
    for (size_t i = 0; i < NUM_PUTS; ++i) {
        uint64_t key = randkey(gen);
        CheckedPut(key, 7);
    }

    // garner->PrintStats(true);

    // getting keys that should be found
    if (refvec.size() > 0) {
        std::cout << " Testing found Gets..." << std::endl;
        std::uniform_int_distribution<size_t> randidx(0, refvec.size() - 1);
        for (size_t i = 0; i < NUM_FOUND_GETS; ++i) {
            size_t idx = randidx(gen);
            CheckedGet(refvec[idx]);
        }
    }

    // getting keys that should not be found
    std::cout << " Testing not-found Gets..." << std::endl;
    for (size_t i = 0; i < NUM_NOTFOUND_GETS; ++i) {
        uint64_t key;
        do {
            key = randkey(gen);
        } while (refmap.contains(key));
        CheckedGet(key);
    }

    // scanning with tiny ranges
    std::cout << " Testing tiny Scans..." << std::endl;
    for (size_t i = 0; i < NUM_TINY_SCANS; ++i) {
        uint64_t lkey = randkey(gen);
        uint64_t rkey = lkey + 2;
        CheckedScan(lkey, rkey);
    }

    // scanning with normal ranges
    std::cout << " Testing normal Scans..." << std::endl;
    for (size_t i = 0; i < NUM_NORMAL_SCANS; ++i) {
        std::uniform_int_distribution<uint64_t> randlkey(
            0, MAX_KEY - (MAX_KEY / 10));
        uint64_t lkey = randlkey(gen);
        uint64_t rkey = lkey + (MAX_KEY / 10);
        CheckedScan(lkey, rkey);
    }

    // scanning with huge ranges
    std::cout << " Testing huge Scans..." << std::endl;
    for (size_t i = 0; i < NUM_HUGE_SCANS; ++i) {
        std::uniform_int_distribution<uint64_t> randlkey(0, MAX_KEY / 5);
        uint64_t lkey = randlkey(gen);
        uint64_t rkey = lkey + (MAX_KEY - (MAX_KEY / 5));
        CheckedScan(lkey, rkey);
    }

    // garner->PrintStats(true);

    std::cout << " Fuzzy tests passed!" << std::endl;
}

static void fuzzy_tests(const std::string& filename) {
    for (unsigned round = 0; round < 10; ++round) {
        std::cout << "Round " << round << " --" << std::endl;
        fuzzy_test_round(filename, round != 0);
    }
}

int main(int argc, char* argv[]) {
    bool help;
    std::string file;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "f,file", "backing file",
        cxxopts::value<std::string>(file)->default_value(""));
    auto result = cmd_args.parse(argc, argv);

    if (help) {
        printf("%s", cmd_args.help().c_str());
        return 0;
    }

    if (file.length() == 0) {
        std::cerr << "Error: backing file path is empty" << std::endl;
        printf("%s", cmd_args.help().c_str());
        return 1;
    }

    fuzzy_tests(file);

    return 0;
}
