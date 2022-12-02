CC=g++
CXXFLAGS=-Wall -Werror -std=c++20 -D_GNU_SOURCE
CXXLIBS=-pthread

BPT_SRCS:=$(wildcard ./src/*.cpp)
BPT_HDRS:=$(wildcard ./src/*.hpp)
BPT_OBJS:=$(patsubst %.cpp,%.o,$(BPT_SRCS))
BPT_INCL:=-I./src

CLI_SRCS:=$(wildcard ./client/*.cpp)
CLI_OBJS:=$(patsubst %.cpp,%.o,$(CLI_SRCS))


.PHONY: all
all: release


.PHONY: release
release: CXXFLAGS+=-O3 -DNDEBUG
release: SUBMAKE_TYPE=release
release: bptcli

.PHONY: debug
debug: CXXFLAGS+=-Og -g -ggdb
debug: SUBMAKE_TYPE=debug
debug: bptcli


$(BPT_OBJS): %.o: %.cpp $(BPT_HDRS)
	$(CC) $(CXXFLAGS) -c $(BPT_INCL) $< -o $@ $(CXXLIBS)

$(CLI_OBJS): %.o: %.cpp
	$(CC) $(CXXFLAGS) -c $(BPT_INCL) $< -o $@ $(CXXLIBS)

bptcli: $(CLI_OBJS) $(BPT_OBJS)
	$(CC) $(CXXFLAGS) $(CLI_OBJS) $(BPT_OBJS) -o $@ $(CXXLIBS)


.PHONY: clean
clean:
	rm -f $(CLI_OBJS) $(BPT_OBJS) bptcli
