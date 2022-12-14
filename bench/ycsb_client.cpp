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
#include "utils.hpp"

static std::tuple<std::vector<GarnerReq>, size_t> read_input_trace(
    const std::string& filename) {
    assert(filename.length() > 0);
    std::vector<GarnerReq> reqs;
    size_t degree = 0;

    std::ifstream input(filename);
    std::string opcode;
    std::string key;
    while (input >> opcode >> key) {
        if (opcode == "DEGREE") {
            // special line indicating the degree parameter of tree
            degree = std::stoul(key);
            continue;
        }
        GarnerOp op = (opcode == "GET")      ? GET
                      : (opcode == "PUT")    ? PUT
                      : (opcode == "DELETE") ? DELETE
                      : (opcode == "SCAN")   ? SCAN
                                             : UNKNOWN;
        std::string rkey;
        if (op == SCAN) input >> rkey;
        reqs.push_back(GarnerReq(op, key, rkey, ""));
    }

    if (reqs.size() == 0) {
        std::cerr << "Error: input trace " << filename << " has no valid lines"
                  << std::endl;
        exit(1);
    }

    return std::make_tuple(reqs, degree);
}

static std::tuple<unsigned, unsigned> execute_input_trace(
    garner::Garner* garner, const std::vector<GarnerReq>& reqs,
    std::string value, std::vector<double>& microsecs) {
    unsigned cnt = 0, not_ok_cnt = 0;
    std::string get_buf;
    bool get_found, delete_found;
    std::vector<std::tuple<std::string, std::string>> scan_buf;
    size_t scan_nrecords;

    for (const auto& req : reqs) {
        try {
            auto time_start = std::chrono::high_resolution_clock::now();
            switch (req.op) {
                case PUT:
                    garner->Put(req.key, value);
                    break;
                case GET:
                    garner->Get(req.key, get_buf, get_found);
                    break;
                case DELETE:
                    garner->Delete(req.key, delete_found);
                    break;
                case SCAN:
                    scan_buf.clear();
                    time_start = std::chrono::high_resolution_clock::now();
                    garner->Scan(req.key, req.rkey, scan_buf, scan_nrecords);
                    break;
                case UNKNOWN:
                default:
                    std::cerr << "Error: unrecognized opcode" << std::endl;
                    exit(1);
            }
            auto time_end = std::chrono::high_resolution_clock::now();

            // record timing for all successful requests
            cnt++;
            microsecs.push_back(
                std::chrono::duration<double, std::micro>(time_end - time_start)
                    .count());

        } catch (const std::exception& ex) {
            std::cerr << "Caught: " << ex.what() << std::endl;
            not_ok_cnt++;
        }
    }

    return std::make_tuple(cnt, not_ok_cnt);
}

static void print_results_latency(std::vector<double>& microsecs) {
    if (microsecs.size() > 0) {
        std::sort(microsecs.begin(), microsecs.end());

        std::cout << "Sorted time elapsed:";
        for (double& us : microsecs) std::cout << " " << us;
        std::cout << std::endl << std::endl;

        if (microsecs.size() > 1)
            microsecs.erase(microsecs.end() - 1, microsecs.end());

        double sum_us = 0.;
        for (double& us : microsecs) sum_us += us;
        double min_us = microsecs.front();
        double max_us = microsecs.back();
        double avg_us = sum_us / microsecs.size();

        std::cout << "Time elapsed stats:" << std::endl
                  << std::fixed << std::setprecision(3) << "  sum  " << sum_us
                  << " us" << std::endl
                  << "  avg  " << avg_us << " us" << std::endl
                  << "  max  " << max_us << " us" << std::endl
                  << "  min  " << min_us << " us" << std::endl
                  << std::endl;
    }
}

int main(int argc, char* argv[]) {
    bool help;
    std::string trace;

    cxxopts::Options cmd_args(argv[0]);
    cmd_args.add_options()("h,help", "print help message",
                           cxxopts::value<bool>(help)->default_value("false"))(
        "w,workload", "workload trace",
        cxxopts::value<std::string>(trace)->default_value(""));
    auto result = cmd_args.parse(argc, argv);

    if (help) {
        printf("%s", cmd_args.help().c_str());
        return 0;
    }

    if (trace.length() == 0) {
        std::cerr << "Error: workload trace is empty" << std::endl;
        printf("%s", cmd_args.help().c_str());
        return 1;
    }

    std::vector<GarnerReq> reqs;
    size_t degree = 0;
    std::tie(reqs, degree) = read_input_trace(trace);
    std::string value = "ABCDEFGHIJ";

    auto* gn = garner::Garner::Open(degree, garner::PROTOCOL_NONE);
    std::vector<double> microsecs;
    auto [cnt, _] = execute_input_trace(gn, reqs, std::move(value), microsecs);
    std::cout << "Finished " << cnt << " requests." << std::endl << std::endl;

    print_results_latency(microsecs);
    garner::BPTreeStats stats = gn->GatherStats(false);
    std::cout << stats << std::endl;

    delete gn;
    return 0;
}
