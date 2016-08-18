//
// Copyright(c) 2015-2016 Vinnie Falco(vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0.(See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_DETAIL_WIN32_FILE_HPP
#define NUDB_DETAIL_WIN32_FILE_HPP

#include <nudb/common.hpp>
#include <nudb/error.hpp>
#include <cassert>
#include <climits>
#include <string>

#ifndef NUDB_WIN32_FILE
# ifdef _MSC_VER
#  define NUDB_WIN32_FILE 1
# else
#  define NUDB_WIN32_FILE 0
# endif
#endif

#if NUDB_WIN32_FILE
#pragma push_macro("NOMINMAX")
#pragma push_macro("UNICODE")
#pragma push_macro("STRICT")
# ifndef NOMINMAX
#  define NOMINMAX
# endif
# ifndef UNICODE
#  define UNICODE
# endif
# ifndef STRICT
#  define STRICT
# endif
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif
# include <Windows.h>
#pragma pop_macro("STRICT")
#pragma pop_macro("UNICODE")
#pragma pop_macro("NOMINMAX")
#endif

#if NUDB_WIN32_FILE

namespace nudb {
namespace detail {

// Win32 error code
class file_win32_error
    : public file_error
{
public:
    explicit
    file_win32_error(char const* m,
            DWORD dwError = ::GetLastError())
        : file_error(std::string("nudb: ") + m +
            ", " + text(dwError))
    {
    }

    explicit
    file_win32_error(std::string const& m,
            DWORD dwError = ::GetLastError())
        : file_error(std::string("nudb: ") + m +
            ", " + text(dwError))
    {
    }

private:
    template<class = void>
    static
    std::string
    text(DWORD dwError);
};

template<class>
std::string
file_win32_error::text(DWORD dwError)
{
    LPSTR buf = nullptr;
    size_t const size = FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER |
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        dwError,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
       (LPSTR)&buf,
        0,
        NULL);
    std::string s;
    if(size)
    {
        s.append(buf, size);
        LocalFree(buf);
    }
    else
    {
        s = "error " + std::to_string(dwError);
    }
    return s;
}

} // detail

//------------------------------------------------------------------------------

class win32_file
{
    HANDLE hf_ = INVALID_HANDLE_VALUE;

public:
    win32_file() = default;
    win32_file(win32_file const&) = delete;
    win32_file& operator=(win32_file const&) = delete;

    /** Destructor.

        If open, the file is closed.
    */
    ~win32_file();

    /** Move constructor.

        @note The state of the moved-from object is as if default constructed.
    */
    win32_file(win32_file&&);

    /** Move assignment.

        @note The state of the moved-from object is as if default constructed.
    */
    win32_file&
    operator=(win32_file&& other);

    /// Returns `true` if the file is open.
    bool
    is_open() const
    {
        return hf_ != INVALID_HANDLE_VALUE;
    }

    /// Close the file if it is open.
    void
    close();

    //  Returns:
    //      `false` if the file already exists
    //      `true` on success, else throws
    //
    bool
    create(file_mode mode, path_type const& path);

    /** Create a new file.

        After the file is created, it is opened as if by open(mode, path, ec).

        Preconditions:
            The file must not already exist, or errc::file_exists is returned.

        @param mode The open mode.

        @param path The path of the file to create.

        @param ec Set to the error, if any occurred.
    */
    void
    create(file_mode mode, path_type const& path, error_code& ec);

    //  Returns:
    //      `false` if the file doesnt exist
    //      `true` on success, else throws
    //
    bool
    open(file_mode mode, path_type const& path);

    /** Open a file.

        @param mode The open mode.

        @param path The path of the file to open.

        @param ec Set to the error, if any occurred.
    */
    void
    open(file_mode mode, path_type const& path, error_code& ec);

    //  Effects:
    //      Removes the file from the file system.
    //
    //  Throws:
    //      Throws is an error occurs.
    //
    //  Returns:
    //      `true` if the file was erased
    //      `false` if the file was not present
    //
    static
    bool
    erase(path_type const& path);

    /** Remove a file from the file system.

        Preconditions:
            The file must exist.

        @param path The path of the file to remove.

        @param ec Set to the error, if any occurred.
    */
    static
    void
    erase(path_type const& path, error_code& ec);

    //  Returns:
    //      Current file size in bytes measured by operating system
    //  Requires:
    //      is_open() == true
    //
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
    read(std::size_t offset, void* buffer, std::size_t bytes);

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
    read(std::size_t offset,
        void* buffer, std::size_t bytes, error_code& ec);

    void
    write(std::size_t offset,
        void const* buffer, std::size_t bytes);

    /** Write data to a location in the file.

        Preconditions:
            The file must be open with a write mode.

        @param offset The position in the file to write from,
        expressed as a byte offset from the beginning.

        @param buffer The data the write.

        @param bytes The number of bytes to write.

        @param ec Set to the error, if any occurred.
    */
    void
    write(std::size_t offset,
        void const* buffer, std::size_t bytes, error_code& ec);

    void
    sync();

    /** Perform a low level file synchronization.

        Preconditions:
            The file must be open with a write mode.

        @param ec Set to the error, if any occurred.
    */
    void
    sync(error_code& ec);

    void
    trunc(std::size_t length);

    /** Truncate the file at a specific size.

        Preconditions:
            The file must be open with a write mode.

        @param length The new file size.

        @param ec Set to the error, if any occurred.
    */
    void
    trunc(std::size_t length, error_code& ec);

private:
    static
    void
    err(DWORD dwError, error_code& ec)
    {
        ec = error_code{static_cast<int>(dwError), system_category()};
    }

    static
    void
    last_err(error_code& ec)
    {
        err(::GetLastError(), ec);
    }

    static
    std::pair<DWORD, DWORD>
    flags(file_mode mode);
};

} // nudb

#include <nudb/impl/win32_file.ipp>

#endif

#endif
