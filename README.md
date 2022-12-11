# Garner

[![Format check](https://github.com/josehu07/garner/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/garner/actions?query=josehu07%3Aformat)
[![Build & Tests status](https://github.com/josehu07/garner/actions/workflows/build-n-tests.yml/badge.svg)](https://github.com/josehu07/garner/actions?query=josehu07%3Abuild_n_tests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Hierarchical validation in Silo-flavor optimistic concurrency control (OCC) on a B+-tree index.

## Build

Install dependencies and `gcc` 11.x for full C++-20 support:

```bash
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt update
sudo apt upgrade

sudo apt install build-essential gcc-11 g++-11 cpp-11 cmake clang-format

sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
sudo update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 100
```

Build Garner library & client executables:

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

Build in debug mode:

```bash
mkdir build-debug && cd build-debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j
```

## Run

Run all tests (recommend release mode build):

```bash
ctest
```

Run an individual test for detailed output:

```bash
./tests/test_<name> -h
```

TODO

## TODO List

- [x] Basic concurrent BPTree
- [x] Transaction manager
- [ ] Basic HV-OCC protocol
- [ ] Simple benchmarking
- [ ] Try jemalloc/tcmalloc
- [ ] Do better than comparing keys in skipping children nodes
- [ ] Properly support on-the-fly insertions
- [ ] Better latching to reduce root contention
- [ ] Remove shared_mutex in cases where an atomic is fine
- [ ] Replace shared_mutex with userspace spinlock
- [ ] Implement Delete & related concurrency

## References

- [Latch crabbing (Latch coupling)](https://15445.courses.cs.cmu.edu/fall2018/slides/09-indexconcurrency.pdf)
- [Silo OCC](https://dl.acm.org/doi/10.1145/2517349.2522713)
