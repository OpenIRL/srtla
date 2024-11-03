cmake_minimum_required(VERSION 3.16)
project(srtla_rec VERSION 1.0.0)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/Modules")

find_package(spdlog REQUIRED)

add_executable(srtla_rec
    src/common.c
    src/common.h
    src/main.cpp
    src/main.h)

target_include_directories(srtla_rec PRIVATE
	"deps/argparse/include")

target_link_libraries(srtla_rec PRIVATE
    spdlog::spdlog)

target_compile_features(srtla_rec PRIVATE cxx_std_17)
#target_compile_options(srtla_rec PRIVATE -Wall -Wextra)
target_compile_definitions(srtla_rec PUBLIC VERSION="${CMAKE_PROJECT_VERSION}")

set(CMAKE_BUILD_TYPE RelWithDebInfo)
install(TARGETS srtla_rec)
