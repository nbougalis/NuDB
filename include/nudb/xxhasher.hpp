//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_XXHASHER_HPP
#define NUDB_XXHASHER_HPP

#include <nudb/detail/xxhash.hpp>
#include <cstddef>
#include <type_traits>

namespace nudb {

class xxhasher
{
    // requires 64-bit std::size_t
    static_assert(sizeof(std::size_t)==8, "");

    XXH64_state_s state_;

    XXH64_state_t*
    state()
    {
        return reinterpret_cast<XXH64_state_t*>(&state_);
    }

public:
    using result_type = std::size_t;

    xxhasher() noexcept
    {
        XXH64_reset(state(), 1);
    }

    template<class Seed,
        typename std::enable_if<
            std::is_unsigned<Seed>::value>::type* = nullptr>
    explicit
    xxhasher(Seed seed)
    {
        XXH64_reset(state(), seed);
    }

    template<class Seed,
        typename std::enable_if<
            std::is_unsigned<Seed>::value>::type* = nullptr>
    xxhasher(Seed seed, Seed)
    {
        XXH64_reset(state(), seed);
    }

    void
    operator()(void const* key, std::size_t len) noexcept
    {
        XXH64_update(state(), key, len);
    }

    explicit
    operator std::size_t() noexcept
    {
        return XXH64_digest(state());
    }
};

} // nudb

#endif
