# Part of nudb

GroupSources(include/nudb nudb)

GroupSources(test "/")

add_executable(test-all
    ${NUDB_INCLUDES}
    api.cpp
    callgrind_test.cpp
    common.cpp
    create.cpp
    file.cpp
    identity.cpp
    posix_file.cpp
    recover.cpp
    recover_test.cpp
    store.cpp
    store_test.cpp
    varint_test.cpp
    verify.cpp
    visit.cpp
    win32_file.cpp
    ../include/nudb/detail/xxhash.cpp
    ../deps/beast/extras/beast/unit_test/main.cpp
)

if (WIN32)
    target_link_libraries(test-all ${Boost_LIBRARIES})
else ()
    target_link_libraries(test-all ${Boost_LIBRARIES} Threads::Threads)
endif ()
