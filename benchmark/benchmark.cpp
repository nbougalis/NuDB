//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include "test_util.hpp"

#if WITH_ROCKSDB
#include "rocksdb/db.h"
#endif

#include <array>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <map>
#include <memory>
#include <random>
#include <utility>

struct Timer
{
    using clock = std::chrono::high_resolution_clock;
    using time_point = clock::time_point;
    time_point start_;

    Timer() : start_(clock::now())
    {
    }

    void
    reset()
    {
        start_ = clock::now();
    }

    auto
    elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::duration<double>>(
            clock::now() - start_);
    }
};

template <class Generator, class F>
std::chrono::duration<double>
time_block(size_t n, Generator&& g, F&& f)
{
    Timer timer;
    while(n--)
    {
        f(g());
    }
    return timer.elapsed();
}

#if WITH_ROCKSDB
std::map<std::string, std::chrono::duration<double>>
do_timings_rocks(std::size_t N)
{
    std::map<std::string, std::chrono::duration<double>> result;
    nudb::test::temp_dir td;

    std::unique_ptr<rocksdb::DB> pdb = [path = td.path()]
    {
        rocksdb::DB* db = nullptr;
        rocksdb::Options options;
        options.create_if_missing = true;
        auto const status = rocksdb::DB::Open(options, path, &db);
        if (!status.ok())
            db = nullptr;
        return std::unique_ptr<rocksdb::DB>{db};
    }();

    if (!pdb)
    {
        std::cerr << "Failed to open rocks db.\n";
        return result;
    }

    auto get_generator = [](size_t start) {
        return [cur = start]() mutable
        {
            nudb::test::Sequence seq;
            return seq[cur++];
        };
    };

    auto inserter = [&db = *pdb](auto const& v)
    {
        db.Put(rocksdb::WriteOptions(),
            rocksdb::Slice(
                   reinterpret_cast<char const*>(&v.key), sizeof(v.key)),
            rocksdb::Slice(reinterpret_cast<char const*>(v.data), v.size));
    };

    auto fetcher = [&db = *pdb](auto const& v)
    {
        std::string value;
        db.Get(rocksdb::ReadOptions(),
            rocksdb::Slice(
                   reinterpret_cast<char const*>(&v.key), sizeof(v.key)),
            &value);
    };

    auto insert_fetch =
        [&inserter, &fetcher, g = get_generator(N) ](auto const& v) mutable
    {
        fetcher(std::forward<decltype(v)>(v));
        inserter(g());
    };

    result["insert"] = time_block(N, get_generator(0), inserter);
    result["fetch"] = time_block(N, get_generator(0), fetcher);
    result["insert_dups"] =
            time_block(N, get_generator(0), inserter);
    result["insert_fetch"] =
        time_block(N, get_generator(0), insert_fetch);

    return result;
}
#endif

std::map<std::string, std::chrono::duration<double>>
do_timings(std::size_t N, std::size_t block_size, float load_factor)
{
    std::map<std::string, std::chrono::duration<double>> result;

    using api =
        nudb::api<nudb::test::xxhasher, nudb::identity, nudb::native_file>;

    nudb::test::temp_dir td;

    auto const dp = td.file("nudb.dat");
    auto const kp = td.file("nudb.key");
    auto const lp = td.file("nudb.log");
    nudb::test::Sequence seq;
    api::store db;
    try
    {
        api::create(dp, kp, lp, nudb::test::appnum, nudb::test::salt,
            sizeof(nudb::test::key_type), block_size, load_factor);
        db.open(dp, kp, lp, nudb::test::arena_alloc_size);

        auto get_generator = [](size_t start) {
            return [cur = start]() mutable
            {
                nudb::test::Sequence seq;
                return seq[cur++];
            };
        };
        auto inserter = [&db](
            auto const& v) {db.insert(&v.key, v.data, v.size);};
        // Storage is not copyable, so can't use generalized capture `[storage =
        // nudb::test::Storage{}]`
        nudb::test::Storage storage;
        auto fetcher = [&db, &storage](
            auto const& v) { db.fetch(&v.key, storage); };
        auto insert_fetch =
            [&inserter, &fetcher, g = get_generator(N) ](auto const& v) mutable
        {
            fetcher(std::forward<decltype(v)>(v));
            inserter(g());
        };

        result["insert"] = time_block(N, get_generator(0), inserter);
        result["fetch"] = time_block(N, get_generator(0), fetcher);

        // insert duplicates
        result["insert_dups"] =
            time_block(N, get_generator(0), inserter);
        // insert/fetch
        result["insert_fetch"] =
                time_block(N, get_generator(0), insert_fetch);
        db.close();

    }
    catch (nudb::store_error const& e)
    {
        std::cerr << "Error: " << e.what() << '\n';
    }
    catch (std::exception const& e)
    {
        std::cerr << "Error: " << e.what() << '\n';
    }


    api::file_type::erase(dp);
    api::file_type::erase(kp);
    api::file_type::erase(lp);

    return result;
}

int
main()
{
    enum
    {
        N = 50000,
        block_size = 256
    };

    float const load_factor = 0.95f;

    auto const nudb_timings = do_timings(N, block_size, load_factor);

    auto const col_w = 14;
    auto const db_w = 9;
#if WITH_ROCKSDB
    auto const rocksdb_timings = do_timings_rocks(N);
#endif

    auto tests = {"insert", "fetch", "insert_dups", "insert_fetch"};
    auto print_row = [&tests, col_w](auto const& result) {
        auto const p = std::cout.precision();
        std::cout.precision(3);
        for (auto t : tests)
            std::cout << std::setw(col_w) << result.find(t)->second.count();
        std::cout << '\n';
    };
    std::cout << std::setw(db_w) << " ";
    for (auto t : tests)
        std::cout << std::setw(col_w) << t;
    std::cout << '\n';
    std::cout << std::setw(db_w) << "nudb";
    print_row(nudb_timings);
#if WITH_ROCKSDB
    std::cout << std::setw(db_w) << "rocksdb";
    print_row(rocksdb_timings);
#endif
}
