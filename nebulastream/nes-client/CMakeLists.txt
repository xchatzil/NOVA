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
add_subdirectory(src)
get_source(nes-client NES_CLIENT_SOURCE_FILES)

# Add Library
add_library(nes-client ${NES_CLIENT_SOURCE_FILES})
target_link_libraries(nes-client PRIVATE nes-operators)
target_link_libraries(nes-client PRIVATE cpr::cpr ${CURL_LIBRARIES} nes-grpc nes-common)
target_include_directories(nes-client PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

register_public_header_dir(nes-client ${CMAKE_CURRENT_SOURCE_DIR}/include/API/ API)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes client and headers to cpack
install(TARGETS nes-client
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

include(GNUInstallDirs)
install(DIRECTORY "include/"
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/nebulastream/)