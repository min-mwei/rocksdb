include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

PLATFORM_LDFLAGS += -L/usr/local/lib -L/usr/local/Cellar/openssl/1.0.2j/lib -lboost_system -lssl -lcrypto -lcpprest -lazurestorage -lboost_system -lboost_thread -lboost_chrono

.PHONY: clean librocksdb

all: raiddb

raiddb: librocksdb raiddb.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../librocksdb.a -I../include -I ../ -g -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ -I../include

clean:
	rm -rf ./raiddb *.dSYM
