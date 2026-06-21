cmake_minimum_required(VERSION 3.16)
project(srtla_rec VERSION 1.0.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/Modules")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(spdlog REQUIRED)

# Core library: shared code extracted from the monolithic main.cpp.
# This target grows as further modules are split out (net, conn, registry, packet).
add_library(srtla_core STATIC
    src/proto/protocol.cpp)
target_include_directories(srtla_core PUBLIC src)
target_link_libraries(srtla_core PUBLIC spdlog::spdlog)
target_compile_features(srtla_core PUBLIC cxx_std_17)

# The daemon: thin entrypoint wiring the core together.
add_executable(srtla_rec src/main.cpp)
target_include_directories(srtla_rec PRIVATE deps/argparse/include)
target_link_libraries(srtla_rec PRIVATE srtla_core)
target_compile_definitions(srtla_rec PRIVATE VERSION="${CMAKE_PROJECT_VERSION}")

install(TARGETS srtla_rec)
