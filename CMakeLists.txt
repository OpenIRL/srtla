cmake_minimum_required(VERSION 3.16)
project(irltk_srtla_rec VERSION 1.0.0)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/Modules")

# Find nlohmann JSON
find_package(nlohmann_json 3 REQUIRED)

add_executable(irltk_srtla_rec
    src/common.c
    src/common.h
    src/main.cpp
    src/main.h)

target_link_libraries(irltk_srtla_rec PRIVATE
    nlohmann_json::nlohmann_json)

target_compile_features(irltk_srtla_rec PRIVATE cxx_std_17)
#target_compile_options(irltk_srtla_rec PRIVATE -Wall -Wextra)
target_compile_definitions(irltk_srtla_rec PUBLIC VERSION="${CMAKE_PROJECT_VERSION}")

set(CMAKE_BUILD_TYPE RelWithDebInfo)
install(TARGETS irltk_srtla_rec)
