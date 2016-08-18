//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_ERROR_HPP
#define NUDB_ERROR_HPP

#include <boost/system/system_error.hpp>
#include <boost/system/error_code.hpp>

namespace nudb {

namespace errc = boost::system::errc;
using error_code = boost::system::error_code;
using boost::system::system_category;
using boost::system::generic_category;

/// Database error codes.
enum class error
{
    /// No error
    success = 0,

    /// A file read returned less data than expected
    short_read,

    /// A file write stored less data than expected
    short_write
};

/// The error category used for database error codes.
struct error_category : public boost::system::error_category
{
    char const*
    name() const noexcept override
    {
        return "nudb";
    }

    std::string
    message(int ev) const override
    {
        switch(static_cast<error>(ev))
        {
        case error::short_read:
            return "short read";

        case error::short_write:
            return "short write";

        default:
            return "database error";
        }
    }

    boost::system::error_condition
    default_error_condition(int ev) const noexcept override
    {
        return boost::system::error_condition(ev, *this);
    }

    bool
    equivalent(int ev,
        boost::system::error_condition const& condition) const noexcept override
    {
        return condition.value() == ev &&
            &condition.category() == this;
    }

    bool
    equivalent(error_code const& error, int ev) const noexcept override
    {
        return error.value() == ev && &error.category() == this;
    }
};

/// Returns the error category used for database error codes.
inline
boost::system::error_category const&
get_error_category()
{
    static error_category const cat{};
    return cat;
}

/// Returns a database error code.
inline
boost::system::error_code
make_error_code(error ev)
{
    return error_code{static_cast<int>(ev), get_error_category()};
}

} // nudb

namespace boost {
namespace system {
template<>
struct is_error_code_enum<nudb::error>
{
    static bool const value = true;
};
} // system
} // boost

#endif
