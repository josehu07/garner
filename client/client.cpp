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

// TODO: uncomment below after Garner interface has been decided.

/** Fuzzy testing stuff. */
class FuzzyTestException : public std::exception {
    std::string what_msg;

   public:
    FuzzyTestException(std::string&& what_msg) : what_msg(what_msg) {}
    ~FuzzyTestException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

static void fuzzy_test_round(const std::string& filename, size_t num_to_load) {
    // constexpr size_t TEST_DEGREE = 6;
    // constexpr uint64_t MAX_KEY = 1200;
    // constexpr size_t NUM_PUTS = 6 * 6 * 6 * 2;
    // constexpr size_t NUM_FOUND_GETS = 15;
    // constexpr size_t NUM_NOTFOUND_GETS = 5;
    // constexpr size_t NUM_TINY_SCANS = 10;
    // constexpr size_t NUM_NORMAL_SCANS = 7;
    // constexpr size_t NUM_HUGE_SCANS = 3;

    // std::filesystem::remove(filename);
    // garner::BPTree<uint64_t, uint64_t> bptree(filename, TEST_DEGREE);

    // std::srand(std::time(NULL));
    // std::random_device rd;
    // std::mt19937 gen(rd());
    // std::uniform_int_distribution<uint64_t> randkey(0, MAX_KEY);

    // std::map<uint64_t, uint64_t> refmap;
    // std::vector<uint64_t> refvec;

    // auto CheckedPut = [&](uint64_t key, uint64_t val) {
    //     // std::cout << "Put " << key << " " << val << std::endl;
    //     bptree.Put(key, val);
    //     refmap[key] = val;
    //     refvec.push_back(key);
    // };
    // auto CheckedGet = [&](uint64_t key) {
    //     uint64_t val = 0, refval = 1;
    //     bool found = false, reffound = false;
    //     // std::cout << "Get " << key;
    //     found = bptree.Get(key, val);
    //     // if (found)
    //     //     std::cout << " found " << val << std::endl;
    //     // else
    //     //     std::cout << " not found" << std::endl;
    //     reffound = refmap.contains(key);
    //     if (reffound) refval = refmap[key];
    //     if (reffound != found) {
    //         throw FuzzyTestException(
    //             "Get mismatch: key=" + std::to_string(key) + " found=" +
    //             (found ? "T" : "F") + " reffound=" + (found ? "T" : "F"));
    //     } else if (found && reffound && refval != val) {
    //         throw FuzzyTestException(
    //             "Get mismatch: key=" + std::to_string(key) + " val=" +
    //             std::to_string(val) + " refval=" + std::to_string(refval));
    //     }
    // };
    // auto CheckedLoad =
    //     [&](std::vector<std::tuple<uint64_t, uint64_t>>& records) {
    //         // std::cout << "Load " << records.size() << std::endl;
    //         bptree.Load(records);
    //         for (auto&& [key, val] : records) {
    //             if (!refmap.contains(key)) {
    //                 refmap[key] = val;
    //                 refvec.push_back(key);
    //             }
    //         }
    //     };
    // auto CheckedScan = [&](uint64_t lkey, uint64_t rkey) {
    //     std::vector<std::tuple<uint64_t, uint64_t>> results, refresults;
    //     size_t nrecords = 0, refnrecords = 0;
    //     // std::cout << "Scan " << lkey << "-" << rkey;
    //     nrecords = bptree.Scan(lkey, rkey, results);
    //     // std::cout << " got " << nrecords << std::endl;
    //     for (auto&& it = refmap.lower_bound(lkey);
    //          it != refmap.upper_bound(rkey); ++it) {
    //         refresults.push_back(std::make_tuple(it->first, it->second));
    //         refnrecords++;
    //     }
    //     if (refnrecords != nrecords) {
    //         throw FuzzyTestException(
    //             "Scan mismatch: lkey=" + std::to_string(lkey) + " rkey=" +
    //             std::to_string(rkey) + " nrecords=" +
    //             std::to_string(nrecords) + " refnrecords=" +
    //             std::to_string(refnrecords));
    //     } else {
    //         for (size_t i = 0; i < nrecords; ++i) {
    //             auto [key, val] = results[i];
    //             auto [refkey, refval] = refresults[i];
    //             if (refkey != key) {
    //                 throw FuzzyTestException(
    //                     "Scan mismatch: lkey=" + std::to_string(lkey) +
    //                     " rkey=" + std::to_string(rkey) +
    //                     " key=" + std::to_string(key) +
    //                     " refkey=" + std::to_string(refkey));
    //             } else if (refval != val) {
    //                 throw FuzzyTestException(
    //                     "Scan mismatch: lkey=" + std::to_string(lkey) +
    //                     " rkey=" + std::to_string(rkey) +
    //                     " val=" + std::to_string(val) +
    //                     " refval=" + std::to_string(refval));
    //             }
    //         }
    //     }
    // };

    // // bptree.PrintStats(true);

    // // bulk-loading into the empty tree
    // if (num_to_load > 0) {
    //     std::cout << " Testing bulk Load..." << std::endl;
    //     std::set<uint64_t> allkeys;
    //     std::vector<std::tuple<uint64_t, uint64_t>> records;
    //     for (size_t i = 0; i < num_to_load; ++i) {
    //         uint64_t key;
    //         while (true) {
    //             key = randkey(gen);
    //             if (!allkeys.contains(key)) {
    //                 allkeys.insert(key);
    //                 break;
    //             }
    //         }
    //         records.emplace_back(key, 7);
    //     }
    //     std::sort(records.begin(), records.end(),
    //               [](const std::tuple<uint64_t, uint64_t>& lhs,
    //                  const std::tuple<uint64_t, uint64_t>& rhs) -> bool {
    //                   return std::get<0>(lhs) < std::get<0>(rhs);
    //               });
    //     CheckedLoad(records);
    // }

    // // bptree.PrintStats(true);

    // // putting a bunch of random records
    // std::cout << " Testing random Puts..." << std::endl;
    // for (size_t i = 0; i < NUM_PUTS; ++i) {
    //     uint64_t key = randkey(gen);
    //     CheckedPut(key, 7);
    // }

    // // bptree.PrintStats(true);

    // // getting keys that should be found
    // std::cout << " Testing found Gets..." << std::endl;
    // std::uniform_int_distribution<size_t> randidx(0, refvec.size() - 1);
    // for (size_t i = 0; i < NUM_FOUND_GETS; ++i) {
    //     size_t idx = randidx(gen);
    //     CheckedGet(refvec[idx]);
    // }

    // // getting keys that should not be found
    // std::cout << " Testing not-found Gets..." << std::endl;
    // for (size_t i = 0; i < NUM_NOTFOUND_GETS; ++i) {
    //     uint64_t key;
    //     do {
    //         key = randkey(gen);
    //     } while (refmap.contains(key));
    //     CheckedGet(key);
    // }

    // // scanning with tiny ranges
    // std::cout << " Testing tiny Scans..." << std::endl;
    // for (size_t i = 0; i < NUM_TINY_SCANS; ++i) {
    //     uint64_t lkey = randkey(gen);
    //     uint64_t rkey = lkey + 2;
    //     CheckedScan(lkey, rkey);
    // }

    // // scanning with normal ranges
    // std::cout << " Testing normal Scans..." << std::endl;
    // for (size_t i = 0; i < NUM_NORMAL_SCANS; ++i) {
    //     std::uniform_int_distribution<uint64_t> randlkey(
    //         0, MAX_KEY - (MAX_KEY / 10));
    //     uint64_t lkey = randlkey(gen);
    //     uint64_t rkey = lkey + (MAX_KEY / 10);
    //     CheckedScan(lkey, rkey);
    // }

    // // scanning with huge ranges
    // std::cout << " Testing huge Scans..." << std::endl;
    // for (size_t i = 0; i < NUM_HUGE_SCANS; ++i) {
    //     std::uniform_int_distribution<uint64_t> randlkey(0, MAX_KEY / 5);
    //     uint64_t lkey = randlkey(gen);
    //     uint64_t rkey = lkey + (MAX_KEY - (MAX_KEY / 5));
    //     CheckedScan(lkey, rkey);
    // }

    // bptree.PrintStats(true);

    std::cout << " Fuzzy tests passed!" << std::endl;
}

static void fuzzy_tests(const std::string& filename) {
    const std::vector<size_t> num_to_loads{0, 5, 10, 13, 6 * 6 * 3};
    unsigned round = 0;
    for (size_t num_to_load : num_to_loads) {
        std::cout << "Round " << round << " --" << std::endl;
        fuzzy_test_round(filename, num_to_load);
        round++;
    }
}

// /** Workload trace execution stuff. */
// typedef enum { GET, PUT, DELETE, SCAN, LOAD, UNKNOWN } bptree_op_t;

// typedef struct {
//     bptree_op_t op;
//     uint64_t key;
//     uint64_t rkey;  // only valid for scan
//                     // only valid for load:
//     std::vector<uint64_t>* loadkeys = nullptr;
// } bptree_req_t;

// static std::tuple<std::vector<bptree_req_t>, size_t> read_input_trace(
//     const std::string& filename) {
//     assert(filename.length() > 0);
//     std::vector<bptree_req_t> reqs;
//     size_t degree = 0;

//     std::ifstream input(filename);
//     std::string opcode;
//     uint64_t key;
//     while (input >> opcode >> key) {
//         if (opcode == "DEGREE") {
//             // special line indicating the degree parameter of tree
//             degree = key;
//             continue;
//         }
//         bptree_op_t op = (opcode == "GET")      ? GET
//                          : (opcode == "PUT")    ? PUT
//                          : (opcode == "DELETE") ? DELETE
//                          : (opcode == "SCAN")   ? SCAN
//                          : (opcode == "LOAD")   ? LOAD
//                                                 : UNKNOWN;
//         uint64_t rkey = 0;
//         std::vector<uint64_t>* loadkeys = nullptr;
//         if (op == SCAN)
//             input >> rkey;
//         else if (op == LOAD) {
//             loadkeys = new std::vector<uint64_t>;
//             size_t nrecords = key;
//             size_t loadkey;
//             while (true) {
//                 input >> opcode >> loadkey;
//                 if (opcode == "ENDLOAD") break;
//                 loadkeys->push_back(loadkey);
//             }
//             if (loadkeys->size() != nrecords) {
//                 std::cerr << "Error: load request num records mismatch"
//                           << std::endl;
//                 exit(1);
//             }
//         }
//         reqs.push_back(bptree_req_t{
//             .op = op, .key = key, .rkey = rkey, .loadkeys = loadkeys});
//     }

//     if (degree < 2 || degree > garner::MAXNKEYS) {
//         std::cerr << "Error: invalid degree parameter " << degree <<
//         std::endl; exit(1);
//     }
//     if (reqs.size() == 0) {
//         std::cerr << "Error: input trace " << filename << " has no valid
//         lines"
//                   << std::endl;
//         exit(1);
//     }

//     return std::make_tuple(reqs, degree);
// }

// static std::tuple<unsigned, unsigned> execute_input_trace(
//     garner::BPTree<uint64_t, uint64_t>& bptree,
//     const std::vector<bptree_req_t>& reqs, uint64_t value,
//     std::vector<double>& microsecs) {
//     unsigned cnt = 0, not_ok_cnt = 0;
//     uint64_t get_buf;
//     std::vector<std::tuple<uint64_t, uint64_t>> scan_buf;
//     std::vector<std::tuple<uint64_t, uint64_t>> load_buf;

//     for (const auto& req : reqs) {
//         try {
//             auto time_start = std::chrono::high_resolution_clock::now();
//             switch (req.op) {
//                 case PUT:
//                     bptree.Put(req.key, value);
//                     break;
//                 case GET:
//                     bptree.Get(req.key, get_buf);
//                     break;
//                 case DELETE:
//                     bptree.Delete(req.key);
//                     break;
//                 case SCAN:
//                     scan_buf.clear();
//                     time_start = std::chrono::high_resolution_clock::now();
//                     bptree.Scan(req.key, req.rkey, scan_buf);
//                     break;
//                 case LOAD:
//                     load_buf.clear();
//                     load_buf.reserve(req.loadkeys->size());
//                     for (auto&& loadkey : *req.loadkeys)
//                         load_buf.emplace_back(loadkey, value);
//                     time_start = std::chrono::high_resolution_clock::now();
//                     bptree.Load(load_buf);
//                     break;
//                 case UNKNOWN:
//                 default:
//                     std::cerr << "Error: unrecognized opcode" << std::endl;
//                     exit(1);
//             }
//             auto time_end = std::chrono::high_resolution_clock::now();

//             // record timing for all successful requests
//             cnt++;
//             microsecs.push_back(
//                 std::chrono::duration<double, std::micro>(time_end -
//                 time_start)
//                     .count());

//         } catch (const std::exception& ex) {
//             std::cerr << "Caught: " << ex.what() << std::endl;
//             not_ok_cnt++;
//         }
//     }

//     return std::make_tuple(cnt, not_ok_cnt);
// }

// static void print_results_latency(std::vector<double>& microsecs) {
//     if (microsecs.size() > 0) {
//         std::sort(microsecs.begin(), microsecs.end());

//         std::cout << "Sorted time elapsed:";
//         for (double& us : microsecs) std::cout << " " << us;
//         std::cout << std::endl << std::endl;

//         if (microsecs.size() > 1)
//             microsecs.erase(microsecs.end() - 1, microsecs.end());

//         double sum_us = 0.;
//         for (double& us : microsecs) sum_us += us;
//         double min_us = microsecs.front();
//         double max_us = microsecs.back();
//         double avg_us = sum_us / microsecs.size();

//         std::cout << "Time elapsed stats:" << std::endl
//                   << std::fixed << std::setprecision(3) << "  sum  " <<
//                   sum_us
//                   << " us" << std::endl
//                   << "  avg  " << avg_us << " us" << std::endl
//                   << "  max  " << max_us << " us" << std::endl
//                   << "  min  " << min_us << " us" << std::endl
//                   << std::endl;
//     }
// }

int main(int argc, char* argv[]) {
    bool help, test;
    std::string file, trace;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "f,file", "backing file",
        cxxopts::value<std::string>(file)->default_value(""))(
        "t,test", "run fuzzy tests",
        cxxopts::value<bool>(test)->default_value("false"))(
        "w,workload", "workload trace",
        cxxopts::value<std::string>(trace)->default_value(""));
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

    // if -t given, do fuzzy testing
    if (test) {
        fuzzy_tests(file);
        return 0;
    }

    // otherwise, run the workload trace
    if (trace.length() == 0) {
        std::cerr << "Error: workload trace is empty" << std::endl;
        printf("%s", cmd_args.help().c_str());
        return 1;
    }

    // std::vector<bptree_req_t> reqs;
    // size_t degree = 0;
    // std::tie(reqs, degree) = read_input_trace(trace);
    // uint64_t value = 7;

    // garner::BPTree<uint64_t, uint64_t> bptree(file, degree);
    // std::vector<double> microsecs;
    // auto [cnt, _] = execute_input_trace(bptree, reqs, value, microsecs);
    // std::cout << "Finished " << cnt << " requests." << std::endl <<
    // std::endl;

    // print_results_latency(microsecs);
    // bptree.PrintStats(false);

    // for (auto&& req : reqs) {
    //     if (req.loadkeys != nullptr) delete req.loadkeys;
    // }

    return 0;
}
