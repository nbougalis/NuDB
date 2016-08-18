//
// Copyright(c) 2015-2016 Vinnie Falco(vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.(See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_IMPL_WIN32_FILE_IPP
#define NUDB_IMPL_WIN32_FILE_IPP

namespace nudb {
namespace detail {

template<class _>
win32_file<_>::
~win32_file()
{
    close();
}

template<class _>
win32_file<_>::
win32_file(win32_file&& other)
    : hf_(other.hf_)
{
    other.hf_ = INVALID_HANDLE_VALUE;
}

template<class _>
win32_file<_>&
win32_file<_>::
operator=(win32_file&& other)
{
    if(&other == this)
        return *this;
    close();
    hf_ = other.hf_;
    other.hf_ = INVALID_HANDLE_VALUE;
    return *this;
}

template<class _>
void
win32_file<_>::
close()
{
    if(hf_ != INVALID_HANDLE_VALUE)
    {
        ::CloseHandle(hf_);
        hf_ = INVALID_HANDLE_VALUE;
    }
}

template<class _>
bool
win32_file<_>::
create(file_mode mode, path_type const& path)
{
    assert(! is_open());
    auto const f = flags(mode);
    hf_ = ::CreateFileA(path.c_str(),
        f.first,
        0,
        NULL,
        CREATE_NEW,
        f.second,
        NULL);
    if(hf_ == INVALID_HANDLE_VALUE)
    {
        DWORD const dwError = ::GetLastError();
        if(dwError != ERROR_FILE_EXISTS)
            throw file_win32_error(
                "create file", dwError);
        return false;
    }
    return true;
}

template<class _>
bool
win32_file<_>::
open(file_mode mode, path_type const& path)
{
    assert(! is_open());
    auto const f = flags(mode);
    hf_ = ::CreateFileA(path.c_str(),
        f.first,
        0,
        NULL,
        OPEN_EXISTING,
        f.second,
        NULL);
    if(hf_ == INVALID_HANDLE_VALUE)
    {
        DWORD const dwError = ::GetLastError();
        if(dwError != ERROR_FILE_NOT_FOUND &&
                dwError != ERROR_PATH_NOT_FOUND)
            throw file_win32_error(
                "open file", dwError);
        return false;
    }
    return true;
}

template<class _>
void
win32_file<_>::
open(file_mode mode, path_type const& path, error_code& ec)
{
    assert(! is_open());
    auto const f = flags(mode);
    hf_ = ::CreateFileA(path.c_str(),
        f.first,
        0,
        NULL,
        OPEN_EXISTING,
        f.second,
        NULL);
    if(hf_ == INVALID_HANDLE_VALUE)
        return last_err(ec);
}

template<class _>
bool
win32_file<_>::
erase(path_type const& path)
{
    BOOL const bSuccess =
        ::DeleteFileA(path.c_str());
    if(! bSuccess)
    {
        DWORD dwError = ::GetLastError();
        if(dwError != ERROR_FILE_NOT_FOUND &&
            dwError != ERROR_PATH_NOT_FOUND)
            throw file_win32_error(
                "erase file");
        return false;
    }
    return true;
}

template<class _>
std::size_t
win32_file<_>::
actual_size() const
{
    assert(is_open());
    LARGE_INTEGER fileSize;
    if(! ::GetFileSizeEx(hf_, &fileSize))
        throw file_win32_error(
            "size file");
    return static_cast<std::size_t>(fileSize.QuadPart);
}

template<class _>
std::size_t
win32_file<_>::
size(error_code& ec) const
{
    assert(is_open());
    LARGE_INTEGER fileSize;
    if(! ::GetFileSizeEx(hf_, &fileSize))
    {
        last_err(ec);
        return 0;
    }
    return fileSize.QuadPart;
}

template<class _>
void
win32_file<_>::
read(std::size_t offset, void* buffer, std::size_t bytes)
{
    while(bytes > 0)
    {
        DWORD bytesRead;
        LARGE_INTEGER li;
        li.QuadPart = static_cast<LONGLONG>(offset);
        OVERLAPPED ov;
        ov.Offset = li.LowPart;
        ov.OffsetHigh = li.HighPart;
        ov.hEvent = NULL;
        DWORD amount;
        if(bytes > std::numeric_limits<DWORD>::max())
            amount = std::numeric_limits<DWORD>::max();
        else
            amount = static_cast<DWORD>(bytes);
        BOOL const bSuccess = ::ReadFile(
            hf_, buffer, amount, &bytesRead, &ov);
        if(! bSuccess)
        {
            DWORD const dwError = ::GetLastError();
            if(dwError != ERROR_HANDLE_EOF)
                throw file_win32_error(
                    "read file", dwError);
            throw file_short_read_error();
        }
        if(bytesRead == 0)
            throw file_short_read_error();
        offset += bytesRead;
        bytes -= bytesRead;
        buffer = reinterpret_cast<char*>(
            buffer) + bytesRead;
    }
}

template<class _>
void
win32_file<_>::
read(std::size_t offset, void* buffer, std::size_t bytes, error_code& ec)
{
    while(bytes > 0)
    {
        DWORD bytesRead;
        LARGE_INTEGER li;
        li.QuadPart = static_cast<LONGLONG>(offset);
        OVERLAPPED ov;
        ov.Offset = li.LowPart;
        ov.OffsetHigh = li.HighPart;
        ov.hEvent = NULL;
        DWORD amount;
        if(bytes > std::numeric_limits<DWORD>::max())
            amount = std::numeric_limits<DWORD>::max();
        else
            amount = static_cast<DWORD>(bytes);
        BOOL const bSuccess = ::ReadFile(
            hf_, buffer, amount, &bytesRead, &ov);
        if(! bSuccess)
        {
            DWORD const dwError = ::GetLastError();
            if(dwError != ERROR_HANDLE_EOF)
                return err(dwError, ec);
            ec = make_error_code(error::short_read);
            return;
        }
        if(bytesRead == 0)
        {
            ec = make_error_code(error::short_read);
            return;
        }
        offset += bytesRead;
        bytes -= bytesRead;
        buffer = reinterpret_cast<char*>(
            buffer) + bytesRead;
    }
}

template<class _>
void
win32_file<_>::
write(std::size_t offset, void const* buffer, std::size_t bytes)
{
    while(bytes > 0)
    {
        LARGE_INTEGER li;
        li.QuadPart = static_cast<LONGLONG>(offset);
        OVERLAPPED ov;
        ov.Offset = li.LowPart;
        ov.OffsetHigh = li.HighPart;
        ov.hEvent = NULL;
        DWORD amount;
        if(bytes > std::numeric_limits<DWORD>::max())
            amount = std::numeric_limits<DWORD>::max();
        else
            amount = static_cast<DWORD>(bytes);
        DWORD bytesWritten;
        BOOL const bSuccess = ::WriteFile(
            hf_, buffer, amount, &bytesWritten, &ov);
        if(! bSuccess)
            throw file_win32_error(
                "write file");
        if(bytesWritten == 0)
            throw file_short_write_error();
        offset += bytesWritten;
        bytes -= bytesWritten;
        buffer = reinterpret_cast<
            char const*>(buffer) +
                bytesWritten;
    }
}

template<class _>
void
win32_file<_>::
sync()
{
    BOOL const bSuccess =
        ::FlushFileBuffers(hf_);
    if(! bSuccess)
        throw file_win32_error("sync file");
}

template<class _>
void
win32_file<_>::
trunc(std::size_t length)
{
    LARGE_INTEGER li;
    li.QuadPart = length;
    BOOL bSuccess;
    bSuccess = ::SetFilePointerEx(
        hf_, li, NULL, FILE_BEGIN);
    if(bSuccess)
        bSuccess = ::SetEndOfFile(hf_);
    if(! bSuccess)
        throw file_win32_error("trunc file");
}

template<class _>
std::pair<DWORD, DWORD>
win32_file<_>::
flags(file_mode mode)
{
    std::pair<DWORD, DWORD> result{0, 0};
    switch(mode)
    {
    case file_mode::scan:
        result.first =
            GENERIC_READ;
        result.second =
            FILE_FLAG_SEQUENTIAL_SCAN;
        break;

    case file_mode::read:
        result.first =
            GENERIC_READ;
        result.second =
            FILE_FLAG_RANDOM_ACCESS;
        break;

    case file_mode::append:
        result.first =
            GENERIC_READ | GENERIC_WRITE;
        result.second =
            FILE_FLAG_RANDOM_ACCESS
            //| FILE_FLAG_NO_BUFFERING
            //| FILE_FLAG_WRITE_THROUGH
            ;
        break;

    case file_mode::write:
        result.first =
            GENERIC_READ | GENERIC_WRITE;
        result.second =
            FILE_FLAG_RANDOM_ACCESS;
        break;
    }
    return result;
}

} // detail
} // nudb

#endif
