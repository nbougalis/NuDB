# Part of nudb

GroupSources(include/nudb nudb)

GroupSources(tools "/")

add_executable (nudb
    ${NUDB_INCLUDES}
    nudb.cpp
    ../include/nudb/detail/xxhash.cpp
)

if (WIN32)
    target_link_libraries(nudb ${Boost_LIBRARIES})
else ()
    target_link_libraries(nudb ${Boost_LIBRARIES} Threads::Threads)
endif ()
