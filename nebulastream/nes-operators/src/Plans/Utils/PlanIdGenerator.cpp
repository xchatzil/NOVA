/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Plans/Utils/PlanIdGenerator.hpp>
#include <atomic>

namespace NES {
QueryId PlanIdGenerator::getNextQueryId() {
    static std::atomic_uint64_t id = INITIAL_QUERY_ID.getRawValue();
    return QueryId(id++);
}

SharedQueryId PlanIdGenerator::getNextSharedQueryId() {
    static std::atomic_uint64_t id = INITIAL_SHARED_QUERY_ID.getRawValue();
    return SharedQueryId(id++);
}

DecomposedQueryId PlanIdGenerator::getNextDecomposedQueryPlanId() {
    static std::atomic_uint64_t id = INITIAL_DECOMPOSED_QUERY_PLAN_ID.getRawValue();
    return DecomposedQueryId(id++);
}

}// namespace NES
