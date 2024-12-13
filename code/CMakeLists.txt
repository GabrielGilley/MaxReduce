cmake_minimum_required(VERSION 3.11...3.18)

project(
    Pando
    VERSION 0.1
    LANGUAGES CXX
    DESCRIPTION "A flexible cross-blockchain analysis system"
    )

include(GNUInstallDirs)

# ----------------------------------------------------------------------------
# Force out-of-source
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
if(EXISTS "${LOC_PATH}")
    message(FATAL_ERROR "You likely want to build or install Pando.
    Please build from a new, separate directory:
    `cd build; cmake ..; make install`.
    If you know what you are doing and intend to build in this directory,
    please modify CMakeLists.txt to remove this check.")
endif()

# ----------------------------------------------------------------------------
# Default to a Release build
if (NOT EXISTS ${CMAKE_BINARY_DIR}/CMakeCache.txt)
    if (NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)
    endif()
endif()

# Use these two lines instead of the next two to compile with address sanitizer
#set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address -fsanitize=leak -g -DDEBUG -O0")
#set(CMAKE_CXX_FLAGS_RELEASE "-fsanitize=address -fsanitize=leak -g -O3 -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -DDEBUG -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -march=native")

# ----------------------------------------------------------------------------

# Add dependencies
add_library(rapidjson INTERFACE)
target_include_directories(rapidjson INTERFACE thirdparty/rapidjson/include)

# Add Boost
FIND_PACKAGE( Boost 1.66 COMPONENTS program_options REQUIRED )
INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )

find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})

find_package(pybind11 REQUIRED)

#find_package(ZeroMQ 4.3 REQUIRED)
SET(CMAKE_CXX_STANDARD 17)
SET(ABSL_PROPAGATE_CXX_STD ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
add_subdirectory(thirdparty/abseil-cpp)
set(CMAKE_POSITION_INDEPENDENT_CODE FALSE)

option(VERBOSE "Enable verbose output" OFF)
if (VERBOSE)
    add_definitions(-DVERBOSE)
endif()

# Add Rust as a dependency
set(Rust_COMPILER /usr/bin/rustc)
include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.3.3 # Optionally specify a commit hash, version tag or branch here
)
# Set any global configuration variables such as `Rust_TOOLCHAIN` before this line!
FetchContent_MakeAvailable(Corrosion)

# ----------------------------------------------------------------------------
# Add the main library
add_library(pando INTERFACE)
target_include_directories(pando INTERFACE include/)
target_link_libraries(pando INTERFACE ${PYTHON_LIBRARIES})
target_link_libraries(pando INTERFACE dl stdc++fs boost_context)

# ----------------------------------------------------------------------------
# Add ElGA as a library
# FIXME use cmake so we change versions / enforce dependency / etc.
add_library(elga SHARED elga/types.cpp elga/address.cpp elga/chatterbox.cpp elga/integer_hash.cpp elga/consistenthasher.cpp elga/timer.cpp)
#target_include_directories(elga PUBLIC elga/ ${ZeroMQ_INCLUDE_DIR})
#target_link_libraries(elga PUBLIC ${ZeroMQ_LIBRARY})
target_include_directories(elga PUBLIC elga/)
target_link_libraries(elga PUBLIC zmq)
target_link_libraries(elga PUBLIC pando)
target_link_libraries(elga PUBLIC
        absl::hash
        absl::flat_hash_map
        )
set_property(TARGET elga PROPERTY CXX_STANDARD 17)
set_property(TARGET elga PROPERTY CXX_STANDARD_REQUIRED on)
target_compile_options(elga PRIVATE -Werror -Wall -Wextra)
install (TARGETS elga DESTINATION ${CMAKE_INSTALL_LIBDIR})

# ----------------------------------------------------------------------------
# Add source files
add_subdirectory(src)

# ----------------------------------------------------------------------------
# Add filters
add_subdirectory(filters)

# ----------------------------------------------------------------------------
# Add pybind11
add_subdirectory(pybind11)

# ----------------------------------------------------------------------------
# Enable ctest
enable_testing()

# ----------------------------------------------------------------------------
# Add tests
add_subdirectory(test)
