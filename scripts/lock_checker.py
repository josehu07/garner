#!/usr/bin/env python3
import argparse


class RWLock(object):
    def __init__(self, target, name):
        self.target = target
        self.name = name

    def __eq__(self, other):
        if self.name == other.name:
            assert self.target == other.target
        return self.name == other.name

    def __str__(self):
        return f"Lock<{self.name}>({self.target})"

    def __hash__(self):
        return hash(self.name)


class RWLockOp(object):
    def __init__(self, lock, mode, action):
        self.lock = lock
        self.mode = mode
        self.action = action


def parse_log(input_file):
    print("Parsing log into trace...")
    trace = dict()

    with open(input_file, "r") as fi:
        for line in fi.readlines():
            line = line.strip()

            if "@t" not in line:
                continue
            if "latch" not in line:
                continue

            tid = int(line[line.find("@t:") + 3 : line.find("]")])
            if tid not in trace:
                trace[tid] = []

            line = line[line.find("]") + 2 :]
            segs = line.strip().split()
            action = segs[3]
            if len(segs) > 5:
                action += "_" + segs[5]
            trace[tid].append(RWLockOp(RWLock(segs[0], segs[4]), segs[2], action))

    print(" Done.")
    return trace


def analyze_trace(trace):
    print("Analyzing lock action trace...")

    print("Held locks --")
    for tid in trace:
        print(f" Thread {tid}:")
        held_locks = dict()

        for op in trace[tid]:
            if op.lock not in held_locks:
                if op.action == "release":
                    print(f"  Error: {op.lock} attempted release while not held")
                    exit(1)
                if op.action != "try_acquire_no":
                    held_locks[op.lock] = op.mode
            else:
                if op.action != "release":
                    print(
                        f"  Error: {op.lock} already held {held_locks[op.lock]} upon: {op.mode} {op.action}"
                    )
                    exit(1)
                if op.mode != held_locks[op.lock]:
                    print(
                        f"  Error: {op.lock} release mode mismatch: expect {held_locks[op.lock]} found {op.mode}"
                    )
                    exit(1)
                del held_locks[op.lock]

        for lock in held_locks:
            print(f"  {lock} {held_locks[lock]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", dest="input_file", required=True)
    args = parser.parse_args()

    trace = parse_log(args.input_file)
    analyze_trace(trace)
