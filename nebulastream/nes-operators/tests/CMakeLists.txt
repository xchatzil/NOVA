# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)

# QueryPlanIteratorTest
add_nes_unit_test(plan-iterator-test "UnitTests/Plans/Util/PlanIteratorTest.cpp")
### Query Plan Tests ###
add_nes_unit_test(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")
### Query Plan Builder Tests ###
add_nes_unit_test(query-plan-builder-tests "UnitTests/Plans/Query/QueryPlanBuilderTest.cpp")

set_tests_properties(${Tests} PROPERTIES TIMEOUT 35)

add_library(nes-operators-test-util SHARED
        "src/Util/JavaUdfDescriptorBuilder.cpp")
target_link_libraries(nes-operators-test-util PRIVATE nes-operators)
target_include_directories(nes-operators-test-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")