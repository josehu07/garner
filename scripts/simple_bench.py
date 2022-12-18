#!/usr/bin/env python3
import matplotlib

matplotlib.use("Agg")

import argparse
import subprocess
import os
import matplotlib.pyplot as plt

GARNER_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

PROTOCOLS = ("silo", "silo_hv", "silo_nr")


def simple_bench_path(collect_latency):
    build_dir = "build-stats" if collect_latency else "build"
    return f"{GARNER_DIR}/{build_dir}/bench/simple_bench"


def run_simple_benchmarks(
    scan_percentages, output_prefix, num_threads, collect_latency
):
    print("Running benchmarks matrix...")
    print(f" #threads: {num_threads}")
    for scan_percentage in sorted(scan_percentages):
        for protocol in PROTOCOLS:
            output_filename = f"{output_prefix}-{protocol}-c{scan_percentage}.log"
            with open(output_filename, "w") as output_file:
                options = [
                    "-p",
                    protocol,
                    "-c",
                    str(scan_percentage),
                    "-t",
                    str(num_threads),
                    # DEBUG
                    "-d",
                    str(64),
                    "-w",
                    str(10000),
                ]
                print(f" Running:  scan {scan_percentage:3d}%  {protocol:7s}")
                subprocess.run(
                    [simple_bench_path(collect_latency)] + options,
                    check=True,
                    stderr=subprocess.STDOUT,
                    stdout=output_file,
                )


def parse_results(scan_percentages, output_prefix, collect_latency):
    print("Parsing benchmark results...")
    results = {}
    for protocol in PROTOCOLS:
        results[protocol] = {}

    for scan_percentage in sorted(scan_percentages):
        for protocol in PROTOCOLS:
            result_filename = f"{output_prefix}-{protocol}-c{scan_percentage}.log"
            with open(result_filename, "r") as result_file:
                abort_rates, throughputs = [], []
                exec_times, lock_times, validate_times, commit_times = [], [], [], []

                for line in result_file.readlines():
                    line = line.strip()
                    if line.startswith("Abort rate:"):
                        abort_rate = float(line[line.index("(") + 1 : line.index("%")])
                        assert abort_rate >= 0.0 and abort_rate < 100.0
                        abort_rates.append(abort_rate)
                    elif line.startswith("Throughput:"):
                        throughput = float(
                            line[line.index(":") + 1 : line.index("txns/sec")]
                        )
                        assert throughput > 0.0
                        throughputs.append(throughput)
                    elif line.startswith("Exec time:"):
                        exec_time = float(line[line.index(":") + 1 : line.index("μs")])
                        exec_times.append(exec_time)
                    elif line.startswith("Lock time:"):
                        lock_time = float(line[line.index(":") + 1 : line.index("μs")])
                        lock_times.append(lock_time)
                    elif line.startswith("Validate time:"):
                        validate_time = float(
                            line[line.index(":") + 1 : line.index("μs")]
                        )
                        validate_times.append(validate_time)
                    elif line.startswith("Commit time:"):
                        commit_time = float(
                            line[line.index(":") + 1 : line.index("μs")]
                        )
                        commit_times.append(commit_time)

                assert len(abort_rates) == len(throughputs)
                assert len(throughputs) > 0
                if collect_latency:
                    assert (
                        len(exec_times)
                        == len(lock_times)
                        == len(validate_times)
                        == len(commit_times)
                        == len(throughputs)
                    )

                avg_abort_rate = sum(abort_rates) / len(abort_rates)
                avg_throughput = sum(throughputs) / len(throughputs)
                avg_exec_time = sum(exec_times) / len(exec_times)
                avg_lock_time = sum(lock_times) / len(lock_times)
                avg_validate_time = sum(validate_times) / len(validate_times)
                avg_commit_time = sum(commit_times) / len(commit_times)

                if not collect_latency:
                    print(
                        f" Result:  scan {scan_percentage:3d}%  {protocol:7s}"
                        f"  abort {avg_abort_rate:4.1f}%  {avg_throughput:.2f} txns/sec"
                    )
                    results[protocol][scan_percentage] = {
                        "throughput": avg_throughput,
                    }
                else:
                    print(
                        f" Result:  scan {scan_percentage:3d}%  {protocol:7s}"
                        f"  abort {avg_abort_rate:4.1f}%  {avg_throughput:.2f} txns/sec"
                        f"  exec {avg_exec_time:8.2f} μs"
                        f"  lock {avg_lock_time:8.4f} μs"
                        f"  validate {avg_validate_time:8.4f} μs"
                        f"  commit {avg_commit_time:8.4f} μs"
                    )
                    results[protocol][scan_percentage] = {
                        "throughput": avg_throughput,
                        "exec_time": avg_exec_time,
                        "lock_time": avg_lock_time,
                        "validate_time": avg_validate_time,
                        "commit_time": avg_commit_time,
                    }

    return results


def plot_results(scan_percentages, results, output_prefix, collect_latency):
    protocol_marker = {"silo": "o", "silo_hv": "v", "silo_nr": "x"}
    protocol_color = {"silo": "steelblue", "silo_hv": "orange", "silo_nr": "red"}

    plt.rcParams.update({"font.size": 18})

    for protocol in reversed(PROTOCOLS):
        xs = scan_percentages
        ys = [
            results[protocol][scan_percentage]["throughput"]
            for scan_percentage in scan_percentages
        ]
        assert len(xs) == len(ys)

        ys = list(map(lambda t: t / 1000.0, ys))
        plt.plot(
            xs,
            ys,
            marker=protocol_marker[protocol],
            color=protocol_color[protocol],
            label=protocol,
        )

    plt.ylabel("Throughput (x1000 txns/sec)")
    plt.xlabel("Scan percentage (%)")
    plt.legend()
    plt.tight_layout()

    plt.savefig(f"{output_prefix}-throughput-plot.png", dpi=200)

    if collect_latency:
        for scan_percentage in scan_percentages:
            labels = PROTOCOLS
            exec_times = [
                results[protocol][scan_percentage]["exec_time"]
                for protocol in PROTOCOLS
            ]
            lock_times = [
                results[protocol][scan_percentage]["lock_time"]
                for protocol in PROTOCOLS
            ]
            validate_times = [
                results[protocol][scan_percentage]["validate_time"]
                for protocol in PROTOCOLS
            ]
            commit_times = [
                results[protocol][scan_percentage]["commit_time"]
                for protocol in PROTOCOLS
            ]

            plt.figure(scan_percentage)
            plt.rcParams.update({"font.size": 18})

            plt.barh(labels, exec_times, label="Exec", color="mediumseagreen")
            left_lock = exec_times
            plt.barh(
                labels, lock_times, left=left_lock, label="Phase 1", color="orange"
            )
            left_validate = [
                exec_times[i] + lock_times[i] for i in range(len(PROTOCOLS))
            ]
            plt.barh(
                labels,
                validate_times,
                left=left_validate,
                label="Phase 2",
                color="steelblue",
            )
            left_commit = [
                exec_times[i] + lock_times[i] + validate_times[i]
                for i in range(len(PROTOCOLS))
            ]
            plt.barh(
                labels, commit_times, left=left_commit, label="Phase 3", color="red"
            )

            # plt.xlim((0, 0.025))
            plt.ylabel("Protocol")
            plt.xlabel("Latency (μs)")
            plt.legend()
            plt.tight_layout()
            plt.savefig(f"{output_prefix}-{scan_percentage}-latency-plot.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_prefix", dest="output_prefix", required=True)
    parser.add_argument("-t", "--num_threads", dest="num_threads", default=16)
    parser.add_argument("-l", "--latency", dest="collect_latency", action="store_true")
    parser.add_argument(
        "scan_percentages",
        metavar="C",
        type=int,
        nargs="+",
        help="List of scan percentages to try",
    )
    args = parser.parse_args()

    if args.num_threads <= 0:
        print(f"Error: invalid #threads {args.num_threads}")

    for scan_percentage in args.scan_percentages:
        if scan_percentage < 0 or scan_percentage > 100:
            print(f"Error: invalid scan percentage {scan_percentage}")
            exit(1)

    run_simple_benchmarks(
        args.scan_percentages,
        args.output_prefix,
        args.num_threads,
        args.collect_latency,
    )
    results = parse_results(
        args.scan_percentages, args.output_prefix, args.collect_latency
    )
    plot_results(
        args.scan_percentages, results, args.output_prefix, args.collect_latency
    )
