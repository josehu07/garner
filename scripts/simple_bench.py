#!/usr/bin/env python3
import argparse
import subprocess
import os

GARNER_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PROG_SIMPLE_BENCH = f"{GARNER_DIR}/build/bench/simple_bench"

PROTOCOLS = ("silo", "silo_hv")

def run_simple_benchmarks(scan_percentages, output_prefix):
    print("Running benchmarks matrix...")
    for scan_percentage in sorted(scan_percentages):
        for protocol in PROTOCOLS:
            output_filename = f"{output_prefix}-{protocol}-c{scan_percentage}.log"
            with open(output_filename, 'w') as output_file:
                options = ['-p', protocol, '-c', str(scan_percentage)]
                print(f" Running:  scan {scan_percentage:3d}%  {protocol:7s}")
                subprocess.run([PROG_SIMPLE_BENCH] + options, check=True,
                               stderr=subprocess.STDOUT, stdout=output_file)

def parse_results(scan_percentages, output_prefix):
    print("Parsing benchmark results...")
    for scan_percentage in sorted(scan_percentages):
        for protocol in PROTOCOLS:
            result_filename = f"{output_prefix}-{protocol}-c{scan_percentage}.log"
            with open(result_filename, 'r') as result_file:
                abort_rates, throughputs = [], []
                
                for line in result_file.readlines():
                    line = line.strip()
                    if line.startswith("Abort rate:"):
                        abort_rate = float(line[line.index('(')+1:line.index('%')])
                        assert(abort_rate >= 0. and abort_rate < 100.)
                        abort_rates.append(abort_rate)
                    elif line.startswith("Throughput:"):
                        throughput = float(line[line.index(':')+1:line.index('txns/sec')])
                        assert(throughput > 0.)
                        throughputs.append(throughput)
                assert(len(abort_rates) == len(throughputs))
                assert(len(throughputs) > 0)

                avg_abort_rate = sum(abort_rates) / len(abort_rates)
                avg_throughput = sum(throughputs) / len(throughputs)
                print(f" Result:  scan {scan_percentage:3d}%  {protocol:7s}"
                      f"  abort {avg_abort_rate:4.1f}%  {avg_throughput:.2f} txns/sec")

def plot_results(results):
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output_prefix', dest='output_prefix', required=True)
    parser.add_argument('scan_percentages', metavar='C', type=int, nargs='+',
                        help="List of scan percentages to try")
    args = parser.parse_args()

    for scan_percentage in args.scan_percentages:
        if scan_percentage < 0 or scan_percentage > 100:
            print(f"Error: invalid scan percentage {scan_percentage}")
            exit(1)

    run_simple_benchmarks(args.scan_percentages, args.output_prefix)
    results = parse_results(args.scan_percentages, args.output_prefix)
    # plot_results(results)
