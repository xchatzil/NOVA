# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
# Include dirs
add_subdirectory(src)
get_source(nes-optimizer NES_OPTIMIZER_SOURCE_FILES)

## Find folly
find_package(folly CONFIG REQUIRED)

# Add Library
add_library(nes-optimizer ${NES_OPTIMIZER_SOURCE_FILES})
target_include_directories(nes-optimizer PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
target_link_libraries(nes-optimizer PUBLIC nes-catalogs nes-statistics)

# CPR and CURL for Elegant optimizer
target_link_libraries(nes-optimizer PRIVATE folly::folly cpr::cpr ${CURL_LIBRARIES})

# Z3 Solver for signature computation
find_package(Z3 REQUIRED)
if (NOT ${Z3_FOUND})
    message(FATAL_ERROR "Unable to find Z3")
endif ()
target_link_libraries(nes-optimizer PRIVATE ${Z3_LIBRARIES})
message(STATUS "Using z3: " ${Z3_LIBRARIES})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-optimizer REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-optimizer PUBLIC "-fPIC")
endif ()
if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-optimizer
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)