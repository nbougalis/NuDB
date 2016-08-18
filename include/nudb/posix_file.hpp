//
// Copyright(c) 2015-2016 Vinnie Falco(vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.(See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_DETAIL_POSIX_FILE_HPP
#define NUDB_DETAIL_POSIX_FILE_HPP

#include <nudb/common.hpp>
#include <nudb/error.hpp>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#ifndef NUDB_POSIX_FILE
# ifdef _MSC_VER
#  define NUDB_POSIX_FILE 0
# else
#  define NUDB_POSIX_FILE 1
# endif
#endif

#if NUDB_POSIX_FILE
# include <fcntl.h>
# include <sys/types.h>
# include <sys/uio.h>
# include <sys/stat.h>
# include <unistd.h>
#endif

#if NUDB_POSIX_FILE

namespace nudb {

namespace detail {

class file_posix_error : public file_error
{
public:
    explicit
    file_posix_error(char const* m,
        int errnum = errno)
        : file_error(std::string("nudb: ") + m +
            ", " + text(errnum))
    {
    }

    explicit
    file_posix_error(std::string const& m,
        int errnum = errno)
        : file_error(std::string("nudb: ") + m +
            ", " + text(errnum))
    {
    }

private:
    static
    void
    last_error(error_code& ec)
    {
        ec = error_code{errno, system_category()};
    }

    static
    std::string
    text(int errnum)
    {
        return std::strerror(errnum);
    }
};

//------------------------------------------------------------------------------

template <class = void>
class posix_file
{
private:
    int fd_ = -1;

public:
    posix_file() = default;
    posix_file(posix_file const&) = delete;
    posix_file& operator=(posix_file const&) = delete;

    ~posix_file();

    posix_file(posix_file&&);

    posix_file&
    operator=(posix_file&& other);

    bool
    is_open() const
    {
        return fd_ != -1;
    }

    void
    close();

    bool
    create(file_mode mode, path_type const& path);

    bool
    open(file_mode mode, path_type const& path);

    /** Open a file.

        @param mode The open mode.

        @param path The path of the file to open.

        @param ec Set to the error, if any occurred.
    */
    void
    open(file_mode mode, path_type const& path, error_code& ec);

    static
    bool
    erase(path_type const& path);

    std::size_t
    actual_size() const;

    /** Return the size of the file.

        Preconditions:
            The file must be open.

        @param ec Set to the error, if any occurred.

        @return The size of the file, in bytes.
    */
    std::uint64_t
    size(error_code& ec) const;

    void
    read(std::size_t offset,
        void* buffer, std::size_t bytes);

    /** Read data from a location in the file.

        Preconditions:
            The file must be open.

        @param offset The position in the file to read from,
        expressed as a byte offset from the beginning.

        @param buffer The location to store the data.

        @param bytes The number of bytes to read.

        @param ec Set to the error, if any occurred.
    */
    void
    read(std::size_t offset, void* buffer, std::size_t bytes, error_code& ec);

    void
    write(std::size_t offset,
        void const* buffer, std::size_t bytes);

    void
    sync();

    void
    trunc(std::size_t length);

private:
    static
    void
    err(int ev, error_code& ec)
    {
        ec = error_code{ev, system_category()};
    }

    void
    last_err(error_code& ec)
    {
        err(errno, ec);
    }

    static
    std::pair<int, int>
    flags(file_mode mode);
};

} // detail

using posix_file = detail::posix_file<>;

} // nudb

#include <nudb/impl/posix_file.ipp>

#endif

#endif
