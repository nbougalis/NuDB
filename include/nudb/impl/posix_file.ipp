//
// Copyright(c) 2015-2016 Vinnie Falco(vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.(See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_IMPL_POSIX_FILE_IPP
#define NUDB_IMPL_POSIX_FILE_IPP

namespace nudb {
namespace detail {

inline
posix_file::
~posix_file()
{
    close();
}

inline
posix_file::
posix_file(posix_file &&other)
    : fd_(other.fd_)
{
    other.fd_ = -1;
}

inline
posix_file&
posix_file::
operator=(posix_file&& other)
{
    if(&other == this)
        return *this;
    close();
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
}

inline
void
posix_file::
close()
{
    if(fd_ != -1)
    {
        if(::close(fd_) != 0)
            throw file_posix_error(
                "close file");
        fd_ = -1;
    }
}

inline
bool
posix_file::
create(file_mode mode, path_type const& path)
{
    auto const result = flags(mode);
    assert(! is_open());
    fd_ = ::open(path.c_str(), result.first);
    if(fd_ != -1)
    {
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    int errnum = errno;
    if(errnum != ENOENT)
        throw file_posix_error(
            "open file", errnum);
    fd_ = ::open(path.c_str(),
        result.first | O_CREAT, 0644);
    if(fd_ == -1)
        throw file_posix_error(
            "create file");
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        throw file_posix_error(
            "fadvise");
#endif
    return true;
}

inline
void
posix_file::
create(file_mode mode, path_type const& path, error_code& ec)
{
    auto const result = flags(mode);
    assert(! is_open());
    fd_ = ::open(path.c_str(), result.first);
    if(fd_ != -1)
    {
        ::close(fd_);
        fd_ = -1;
        ec = error_code{errc::file_exists, generic_category());
        return ;
    }
    int errnum = errno;
    if(errnum != ENOENT)
        return err(errnum);
    fd_ = ::open(path.c_str(), result.first | O_CREAT, 0644);
    if(fd_ == -1)
        return last_err(ec);
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        return err(ec);
#endif
}

inline
bool
posix_file::
open(file_mode mode, path_type const& path)
{
    assert(! is_open());
    auto const result = flags(mode);
    fd_ = ::open(path.c_str(), result.first);
    if(fd_ == -1)
    {
        int ev = errno;
        if(ev == ENOENT)
            return false;
        throw file_posix_error(
            "open file", errnum);
    }
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        throw file_posix_error(
            "fadvise");
#endif
    return true;
}

inline
void
posix_file::
open(file_mode mode, path_type const& path, error_code& ec)
{
    assert(! is_open());
    auto const result = flags(mode);
    fd_ = ::open(path.c_str(), result.first);
    if(fd_ == -1)
        return last_err(ec);
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        return last_err(ec);
#endif
}

inline
bool
posix_file::
erase(path_type const& path)
{
    if(::unlink(path.c_str()) != 0)
    {
        int const ec = errno;
        if(ec != ENOENT)
            throw file_posix_error(
                "unlink", ec);
        return false;
    }
    return true;
}

inline
void
posix_file::
erase(path_type const& path, error_code& ec)
{
    if(::unlink(path.c_str()) != 0)
    {
        int const ev = errno;
        if(ev != ENOENT)
            return err(ev);
    }
}

inline
std::size_t
posix_file::
actual_size() const
{
    struct stat st;
    if(::fstat(fd_, &st) != 0)
        throw file_posix_error(
            "fstat");
    return st.st_size;
}

inline
std::size_t
posix_file::
size(error_code& ec) const
{
    struct stat st;
    if(::fstat(fd_, &st) != 0)
    {
        last_err(ec);
        return 0;
    }
    return st.st_size;
}

inline
void
posix_file::
read(std::size_t offset, void* buffer, std::size_t bytes)
{
    while(bytes > 0)
    {
        auto const n = ::pread(
            fd_, buffer, std::min(bytes, SSIZE_MAX), offset);
        if(n == -1)
            throw file_posix_error("pread");
        if(n == 0)
            throw file_short_read_error();
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<
            char*>(buffer) + n;
    }
}

inline
void
posix_file::
read(std::size_t offset, void* buffer, std::size_t bytes, error_code& ec)
{
    while(bytes > 0)
    {
        auto const n = ::pread(
            fd_, buffer, std::min(bytes, SSIZE_MAX), offset);
        if(n == -1)
            return last_err(ec);
        if(n == 0)
            return err(error::short_read, ec);
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<char*>(buffer) + n;
    }
}

inline
void
posix_file::
write(std::size_t offset, void const* buffer, std::size_t bytes)
{
    while(bytes > 0)
    {
        auto const n = ::pwrite(
            fd_, buffer, bytes, offset);
        if(n == -1)
            throw file_posix_error(
                "pwrite");
        if(n == 0)
            throw file_short_write_error();
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<
            char const*>(buffer) + n;
    }
}

inline
void
posix_file::
write(std::size_t offset,
    void const* buffer, std::size_t bytes, error_code& ec)
{
    while(bytes > 0)
    {
        auto const n = ::pwrite(fd_, buffer, bytes, offset);
        if(n == -1)
            return last_err();
        if(n == 0)
            return err(error::short_read, ec);
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<
            char const*>(buffer) + n;
    }
}

inline
void
posix_file::
sync()
{
    if(::fsync(fd_) != 0)
        throw file_posix_error("fsync");
}

inline
void
posix_file::
sync(error_code& ec)
{
    if(::fsync(fd_) != 0)
        return last_err(ec);
}

inline
void
posix_file::
trunc(std::size_t length)
{
    if(::ftruncate(fd_, length) != 0)
        throw file_posix_error("ftruncate");
}

inline
void
posix_file::
trunc(std::size_t length, error_code& ec)
{
    if(::ftruncate(fd_, length) != 0)
        return last_err(ec);
}

inline
std::pair<int, int>
posix_file::
flags(file_mode mode)
{
    std::pair<int, int> result;
    switch(mode)
    {
    case file_mode::scan:
        result.first =
            O_RDONLY;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_SEQUENTIAL;
    #endif
        break;
    case file_mode::read:
        result.first =
            O_RDONLY;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_RANDOM;
    #endif
        break;
    case file_mode::append:
        result.first =
            O_RDWR |
            O_APPEND;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_RANDOM;
    #endif
        break;
    case file_mode::write:
        result.first =
            O_RDWR;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_NORMAL;
    #endif
        break;
    }
    return result;
}

} // detail
} // nudb

#endif
