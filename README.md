# Garner

[![Format check](https://github.com/josehu07/garner/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/garner/actions?query=josehu07%3Aformat)
[![Build & Tests status](https://github.com/josehu07/garner/actions/workflows/build-n-tests.yml/badge.svg)](https://github.com/josehu07/garner/actions?query=josehu07%3Abuild_n_tests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Hierarchical validation in Silo-flavor optimistic concurrency control (OCC) on a B+-tree index (in fact, almost a B-link tree index).

<p align="center">
    <img width="360px" src="HV-OCC.png">
</p>

The Garner codebase is aimed to be a well-documented, expandable, and easy-to-adopt in-memory transactional key-value store for future concurrency control research projects. This is an on-going work and is subject to change.

## Build

<details>
<summary>Install dependencies and `gcc` 11.x for full C++-20 support...</summary>

```bash
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt update
sudo apt upgrade

sudo apt install build-essential gcc-11 g++-11 cpp-11 cmake

sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
sudo update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 100
```
</details>

Build Garner library & client executables:

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

## Run

Run all tests (recommend release mode build):

```bash
cd build
ctest
```

Run an individual test for detailed output:

```bash
./tests/test_<name> -h
```

Run simple benchmarking of transaction throughput:

```bash
./bench/simple_bench -h
```

## Develop

<details>
<summary>Install development dependencies...</summary>

```bash
sudo apt install clang-format python3-pip
pip3 install black matplotlib
```
</details>

Run formatter for all source code files:

```bash
./scripts/format-all.sh
```

Build in debug mode:

```bash
mkdir build-debug && cd build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j
```

Build with certain compile-time options on (e.g., `TXN_STAT`):

```bash
mkdir build-stats && cd build-stats
cmake -DCMAKE_BUILD_TYPE=Release -DTXN_STAT=on ..
make -j
```

## TODO List

- [x] Basic concurrent BPTree
- [x] Transaction manager
- [x] Basic HV-OCC protocol
- [x] Deadlock-free write locking in validation
- [x] Subtree crossing & node item skip_to
- [ ] Proper support for on-the-fly insertions
- [ ] More comprehensive benchmarking
- [ ] Try jemalloc/tcmalloc
- [ ] Better latching to reduce root contention
- [ ] Remove shared_mutex in cases where an atomic is fine
- [ ] Replace shared_mutex with userspace spinlock
- [ ] Start HV protocol at certain level (instead of root)
- [ ] Implement Delete & related concurrency
- [ ] Implement proper durability logging

## References

- [Latch crabbing (Latch coupling)](https://15445.courses.cs.cmu.edu/fall2018/slides/09-indexconcurrency.pdf)
- [Silo OCC](https://dl.acm.org/doi/10.1145/2517349.2522713)
- [Adaptive OCC](http://www.vldb.org/pvldb/vol12/p584-guo.pdf)
