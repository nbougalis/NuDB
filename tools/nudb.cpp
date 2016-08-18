//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <nudb/create.hpp>
#include <nudb/identity.hpp>
#include <nudb/verify.hpp>
#include <nudb/visit.hpp>
#include <nudb/xxhasher.hpp>
#include <nudb/detail/bulkio.hpp>
#include <nudb/detail/format.hpp>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>

namespace nudb {

static std::size_t constexpr readSize = 1 * 1024 * 1024;

/** Create a new key file from a data file.

    This algorithm rebuilds a key file for the given data file.
    It works efficiently by iterating the data file multiple times.
    During the iteration, a contiguous block of the key file is
    rendered in memory, then flushed to disk when the iteration is
    complete. The size of this memory buffer is controlled by the
    bufferSize parameter, larger is better. The algorithm works
    the fastest when bufferSize is large enough to hold the entire
    key file in memory; only a single iteration of the data file
    is needed in this case.

    @param dat_path The path to the data file.

    @param key_path The path to the key file.

    @param itemCount The number of items in the data file.

    @param bufferSize The number of bytes to allocate for the buffer.

    @param progress A function which will be called periodically
    as the algorithm proceeds. The equivalent signature of the
    progress function must be:
    @code
    void progress(
        std::uint64_t amount,   // Amount of work done so far
        std::uint64_t total     // Total amount of work to do
    );
    @endcode

    @tparam The hash function to use.
            Must meet the requirements of Hasher.
*/
template<class Hasher, class Progress>
void
rekey(
    path_type const& dat_path,
    path_type const& key_path,
    std::uint64_t itemCount,
    std::size_t bufferSize,
    Progress const&)
{
    auto const bulk_size = 64 * 1024 * 1024;
    float const load_factor = 0.5;

    auto const dp = dat_path;
    auto const kp = key_path;

    // Create data file with values
    native_file df;
    df.open(file_mode::append, dp);
    detail::dat_file_header dh;
    read(df, dh);
    auto const df_size = df.actual_size();
    detail::bulk_writer<native_file> dw(df, df_size, bulk_size);

    // Create key file
    detail::key_file_header kh;
    kh.version = detail::currentVersion;
    kh.uid = dh.uid;
    kh.appnum = dh.appnum;
    kh.key_size = dh.key_size;
    kh.salt = make_salt();
    kh.pepper = detail::pepper<Hasher>(kh.salt);
    kh.block_size = block_size(kp);
    kh.load_factor = std::min<std::size_t>(
        static_cast<std::size_t>(65536.0 * load_factor), 65535);
    kh.buckets = static_cast<std::size_t>(
        std::ceil(itemCount / (
            detail::bucket_capacity(kh.block_size) * load_factor)));
    kh.modulus = detail::ceil_pow2(kh.buckets);
    native_file kf;
    kf.create(file_mode::append, kp);
    detail::buffer buf(kh.block_size);
    {
        std::memset(buf.get(), 0, kh.block_size);
        detail::ostream os(buf.get(), kh.block_size);
        write(os, kh);
        kf.write(0, buf.get(), kh.block_size);
    }
    // Build contiguous sequential sections of the
    // key file using multiple passes over the data.
    //
    auto const buckets = std::max<std::size_t>(1,
        bufferSize / kh.block_size);
    buf.reserve(buckets * kh.block_size);
    auto const passes =
        (kh.buckets + buckets - 1) / buckets;
#if 0
    log <<
        "buckets: " << kh.buckets << "\n"
        "data:    " << df_size << "\n"
        "passes:  " << passes;
#endif
    //progress p(df_size * passes);
    std::size_t npass = 0;
    for (std::size_t b0 = 0; b0 < kh.buckets;
            b0 += buckets)
    {
        auto const b1 = std::min(
            b0 + buckets, kh.buckets);
        // Buffered range is [b0, b1)
        auto const bn = b1 - b0;
        // Create empty buckets
        for (std::size_t i = 0; i < bn; ++i)
        {
            detail::bucket b(kh.block_size,
                buf.get() + i * kh.block_size, detail::empty);
        }
        // Insert all keys into buckets
        // Iterate Data File
        detail::bulk_reader<native_file> r(df,
            detail::dat_file_header::size, df_size, bulk_size);
        while (! r.eof())
        {
            auto const offset = r.offset();
            // Data Record or Spill Record
            std::size_t size;
            auto is = r.prepare(
                detail::field<detail::uint48_t>::size); // Size
            detail::read<detail::uint48_t>(is, size);
            if (size > 0)
            {
                // Data Record
                is = r.prepare(
                    dh.key_size +           // Key
                    size);                  // Data
                std::uint8_t const* const key =
                    is.data(dh.key_size);
                auto const h = detail::hash<Hasher>(
                    key, dh.key_size, kh.salt);
                auto const n = detail::bucket_index(
                    h, kh.buckets, kh.modulus);
                //p(log, npass * df_size + r.offset());
                if (n < b0 || n >= b1)
                    continue;
                detail::bucket b(kh.block_size, buf.get() +
                    (n - b0) * kh.block_size);
                detail::maybe_spill(b, dw);
                b.insert(offset, size, h);
            }
            else
            {
                // VFALCO Should never get here
                // Spill Record
                is = r.prepare(
                    detail::field<std::uint16_t>::size);
                detail::read<std::uint16_t>(is, size);  // Size
                r.prepare(size); // skip
            }
        }
        kf.write((b0 + 1) * kh.block_size,
            buf.get(), bn * kh.block_size);
        ++npass;
    }
    dw.flush();
    //p.finish(log);
}

//------------------------------------------------------------------------------

int
log2(std::uint64_t n)
{
    int i = -(n == 0);

    auto const S =
        [&](int k)
        {
            if(n >= (std::uint64_t{1} << k))
            {
                i += k;
                n >>= k;
            }
        };
    S(32); S(16); S(8); S(4); S(2); S(1);
    return i;
}

std::string
num(std::uint64_t t)
{
    std::string s = std::to_string(t);
    std::reverse(s.begin(), s.end());
    std::string s2;
    s2.reserve(s.size() + (s.size()+2)/3);
    int n = 0;
    for (auto c : s)
    {
        if (n == 3)
        {
            n = 0;
            s2.insert (s2.begin(), ',');
        }
        ++n;
        s2.insert(s2.begin(), c);
    }
    return s2;
}

namespace detail {

std::ostream&
operator<<(std::ostream& os, dat_file_header const h)
{
    os << std::setfill('0') << std::internal << std::showbase <<
        "type:            '" << std::string{h.type, h.type + sizeof(h.type)} << "'\n"
        "version:         " << h.version << "\n"
        "uid:             " << std::setw(16) << std::hex << h.uid << "\n"
        "appnum:          " << std::setw(16) << std::hex << h.appnum << "\n"
        "key_size:        " << std::dec << h.key_size << "\n"
        ;
    return os;
}

std::ostream&
operator<<(std::ostream& os, key_file_header const h)
{
    os << std::setfill('0') << std::internal << std::showbase <<
        "type:            '" << std::string{h.type, h.type + sizeof(h.type)} << "'\n"
        "version:         " << h.version << "\n"
        "uid:             " << std::setw(16) << std::hex << h.uid << "\n"
        "appnum:          " << std::setw(16) << std::hex << h.appnum << "\n"
        "key_size:        " << h.key_size << "\n"
        "salt:            " << std::setw(16) << std::hex << h.salt << "\n"
        "pepper:          " << std::setw(16) << std::hex << h.pepper << "\n"
        "block_size:      " << num(h.block_size) << "\n"
        ;
    return os;
}

std::ostream&
operator<<(std::ostream& os, log_file_header const h)
{
    os << std::setfill('0') << std::internal << std::showbase <<
        "type:            '" << std::string{h.type, h.type + sizeof(h.type)} << "'\n"
        "version:         " << h.version << "\n"
        "uid:             " << std::setw(16) << std::hex << h.uid << "\n"
        "appnum:          " << std::setw(16) << std::hex << h.appnum << "\n"
        "key_size:        " << h.key_size << "\n"
        "salt:            " << std::setw(16) << std::hex << h.salt << "\n"
        "pepper:          " << std::setw(16) << std::hex << h.pepper << "\n"
        "block_size:      " << num(h.block_size) << "\n"
        "key_file_size:   " << num(h.key_file_size) << "\n"
        "dat_file_size:   " << num(h.dat_file_size) << "\n"
        ;
    return os;
}

} // detail

template<class Hasher>
class admin_tool
{
    int ac_ = 0;
    char const* const* av_ = nullptr;
    boost::program_options::options_description desc_;

public:
    admin_tool()
        : desc_("Options")
    {
        namespace po = boost::program_options;
        desc_.add_options()
            ("buffer,b",    po::value<std::size_t>(),
                            "Set the buffer size in bytes (larger is faster).")
            ("dat,d",       po::value<std::string>(),
                            "Path to data file.")
            ("key,k",       po::value<std::string>(),
                            "Path to key file.")
            ("log,l",       po::value<std::string>(),
                            "Path to log file.")
            ("count",       po::value<std::uint64_t>(),
                            "The number of items in the data file.")
            ("command",     "Command to run.")
            ;
    }

    std::string
    progname() const
    {
        using namespace boost::filesystem;
        return path{av_[0]}.stem().string();
    }

    std::string
    filename(std::string const& s)
    {
        using namespace boost::filesystem;
        return path{s}.filename().string();
    }

    template<class T, std::size_t N>
    static
    std::string
    hist_string(std::array<T, N> const& hist)
    {
        std::size_t n;
        for(n = hist.size() - 1; n > 0; --n)
            if(hist[n])
                break;
        std::string s = std::to_string(hist[0]);
        for(std::size_t i = 1; i <= n; ++i)
            s += ", " + std::to_string(hist[i]);
        return s;
    }

    void
    help()
    {
        std::cout <<
            "usage: " << progname() << " <command> [file...] <options>\n";
        std::cout <<
            "\n"
            "Commands:\n"
            "\n"
            "   help\n"
            "\n"
            "        Print this help information.\n"
            "\n"
            "    info <dat-path> [<key-path> [<log-path>]]\n"
            "\n"
            "        Show metadata and header information for database files.\n"
            "\n"
            "    recover <dat-path> <key-path> <log-path>\n"
            "\n"
            "        Perform a database recovery. A recovery should always be performed first,\n"
            "        before any operations on the database, if a log file is present.\n"
            "\n"
            "    rekey <dat-path] <key-path> <count> --buffer=<bytes>\n"
            "\n"
            "        Generate the key file for a data file. The buffer option is required,\n"
            "        larger buffers process faster. A buffer equal to the size of the key file\n"
            "        processes the fastest. This command must be passed the count of items in\n"
            "        the data file, which can be calculated with the 'visit' command.\n"
            "\n"
            "    verify <dat-path> <key-path> [--buffer=<bytes>]\n"
            "\n"
            "        Verify the integrity of a database. The buffer option is optional,\n"
            "        if omitted a slow algorithm is used. When a buffer size is provided,\n"
            "        a fast algorithm is used with larger buffers resulting in bigger speedups.\n"
            "        A buffer equal to the size of the key file provides the fastest speedup.\n"
            "\n"
            "    visit <dat-path>\n"
            "\n"
            "        Iterate a data file and show information, including the number of\n"
            "        items in the file and a histogram of their log base 2 sizes.\n"
            "\n"
            "Notes:\n"
            "\n"
            "    Paths may be full or relative, and should include the extension. The\n"
            "    recover algorithm should be invoked before running any operation\n"
            "    which can modify the database.\n"
            "\n"
            ;
        desc_.print(std::cout);
    };

    int
    error(std::string const& why)
    {
        std::cerr <<
            progname() << ": " << why << ".\n"
            "Use '" << progname() << " help' for usage.\n";
        return EXIT_FAILURE;
    };

    static
    void
    print(std::ostream& os, verify_info const& info)
    {
        os <<
            "avg_fetch:       " << std::fixed << std::setprecision(3) << info.avg_fetch << "\n" <<
            "waste:           " << std::fixed << std::setprecision(3) << info.waste * 100 << "%" << "\n" <<
            "overhead:        " << std::fixed << std::setprecision(1) << info.overhead * 100 << "%" << "\n" <<
            "actual_load:     " << std::fixed << std::setprecision(0) << info.actual_load * 100 << "%" << "\n" <<
            "version:         " << num(info.version) << "\n" <<
            "uid:             " << std::showbase << std::hex << info.uid << "\n" <<
            "appnum:          " << info.appnum << "\n" <<
            "key_size:        " << num(info.key_size) << "\n" <<
            "salt:            " << std::showbase << std::hex << info.salt << "\n" <<
            "pepper:          " << std::showbase << std::hex << info.pepper << "\n" <<
            "block_size:      " << num(info.block_size) << "\n" <<
            "bucket_size:     " << num(info.bucket_size) << "\n" <<
            "load_factor:     " << std::fixed << std::setprecision(0) << info.load_factor * 100 << "%" << "\n" <<
            "capacity:        " << num(info.capacity) << "\n" <<
            "buckets:         " << num(info.buckets) << "\n" <<
            "key_count:       " << num(info.key_count) << "\n" <<
            "value_count:     " << num(info.value_count) << "\n" <<
            "value_bytes:     " << num(info.value_bytes) << "\n" <<
            "spill_count:     " << num(info.spill_count) << "\n" <<
            "spill_count_tot: " << num(info.spill_count_tot) << "\n" <<
            "spill_bytes:     " << num(info.spill_bytes) << "\n" <<
            "spill_bytes_tot: " << num(info.spill_bytes_tot) << "\n" <<
            "key_file_size:   " << num(info.key_file_size) << "\n" <<
            "dat_file_size:   " << num(info.dat_file_size) << "\n" <<
            "hist:            " << hist_string(info.hist) << "\n"
            ;
        os.flush();
    }

    int
    operator()(int ac, char const* const* av)
    {
        namespace po = boost::program_options;

        ac_ = ac;
        av_ = av;

        auto const progress =
            [](std::uint64_t, std::uint64_t)
            {
            };

        try
        {
            po::positional_options_description pod;
            pod.add("command", 1);
            pod.add("dat", 1);
            pod.add("key", 1);
            pod.add("log", 1);
            pod.add("count", 1);

            po::variables_map vm;
            po::store(po::command_line_parser(ac, av)
                .options(desc_)
                .positional(pod)
                .run()
                ,vm);
            po::notify(vm);

            std::string cmd;

            if(vm.count("command"))
                cmd = vm["command"].as<std::string>();

            if(cmd == "help")
            {
                help();
                return EXIT_SUCCESS;
            }

            if(cmd == "info")
                return do_info(vm);

            if(cmd == "rekey")
            {
                if(! vm.count("dat"))
                    return error("Missing data file path");
                if(! vm.count("key"))
                    return error("Missing key file path");
                if(! vm.count("count"))
                    return error("Missing item count");
                if(! vm.count("buffer"))
                    return error("Missing buffer size");
                auto const dp = vm["dat"].as<std::string>();
                auto const kp = vm["key"].as<std::string>();
                auto const itemCount = vm["count"].as<std::size_t>();
                auto const bufferSize = vm["buffer"].as<std::size_t>();
                rekey<Hasher>(dp, kp, itemCount, bufferSize, progress);
            
                return EXIT_SUCCESS;
            }

            if(cmd == "verify")
            {
                auto const bufferSize = vm.count("buffer") ?
                    vm["buffer"].as<std::size_t>() : 0;
                if(! vm.count("dat"))
                    return error("Missing data file path");
                if(! vm.count("key"))
                    return error("Missing key file path");
                auto const dp = vm["dat"].as<std::string>();
                auto const kp = vm.count("key") ?
                    vm["key"].as<std::string>() : std::string{};
                if(! vm.count("key"))
                {
                    // todo
                }
                else
                {
                    if(bufferSize > 0)
                    {
                        auto const vi = verify_fast<Hasher>(dp, kp, bufferSize, progress);
                        print(std::cout, vi);
                    }
                    else
                    {
                        auto const vi = verify<Hasher>(dp, kp, readSize);
                        print(std::cout, vi);
                    }
                }
                return EXIT_SUCCESS;
            }

            if(cmd == "visit")
            {
                if(! vm.count("dat"))
                    return error("Missing dat path");
                auto const dp = vm["dat"].as<std::string>();
                std::uint64_t n = 0;
                std::array<std::uint64_t, 64> hist;
                hist.fill(0);
                visit<identity>(dp, readSize,
                    [&](void const*, std::size_t,
                        void const*, std::size_t data_size)
                    {
                        ++n;
                        ++hist[log2(data_size)];
                        return true;
                    });
                std::cout <<
                    "data file:       " << dp << "\n"
                    "items:           " << num(n) << "\n" <<
                    hist_string(hist) << "\n";

                return EXIT_SUCCESS;
            }

            return error("Unknown command '" + cmd + "'");
        }
        catch(std::exception const& e)
        {
            return error(e.what());
        }
    }

private:
    int
    do_info(boost::program_options::variables_map const& vm)
    {
        if(! vm.count("dat") && ! vm.count("key") && ! vm.count("log"))
            return error("No files specified");
        if(vm.count("dat"))
            do_info(vm["dat"].as<std::string>());
        if(vm.count("key"))
            do_info(vm["key"].as<std::string>());
        if(vm.count("log"))
            do_info(vm["log"].as<std::string>());
        return EXIT_SUCCESS;
    }

    void
    do_info(path_type const& path)
    {
        error_code ec;
        auto const err =
            [&]
            {
                std::cout << path << ": " << ec.message() << "\n";
            };
        native_file f;
        f.open(file_mode::read, path, ec);
        if(ec)
            return err();
        auto const size = f.size(ec);
        if(ec)
            return err();
        if(size < 8)
        {
            std::cout << "File " << path << " is too small to be a database file.\n";
            return;
        }
        std::array<char, 8> ta;
        f.read(0, ta.data(), ta.size(), ec);
        if(ec)
            return err();
        std::string ts{ta.data(), ta.size()};

        if(ts == "nudb.dat")
        {
            detail::dat_file_header h;
            detail::read(f, h);
            f.close();
            std::cout <<
                "data file:       " << path << "\n"
                "file size:       " << num(size) << "\n" <<
                h << "\n";
            return;
        }

        if(ts == "nudb.key")
        {
            detail::key_file_header h;
            detail::read(f, h);
            f.close();
            std::cout <<
                "key file:        " << path << "\n"
                "file size:       " << num(size) << "\n" <<
                h << "\n";
            return;
        }

        if(ts == "nudb.log")
        {
            detail::log_file_header h;
            detail::read(f, h);
            f.close();
            std::cout <<
                "log file:        " << path << "\n"
                "file size:       " << num(size) << "\n" <<
                h << "\n";
            return;
        }

        std::cout << "File " << path << " has unknown type '" << ts << "'.\n";
    }
};

} // nudb

int
main(int ac, char const* const* av)
{
    using namespace nudb;
    admin_tool<xxhasher> t;
    auto const rv = t(ac, av);
    return rv;
}
