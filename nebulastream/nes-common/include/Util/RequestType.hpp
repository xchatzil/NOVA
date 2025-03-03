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

#ifndef NES_COMMON_INCLUDE_UTIL_REQUESTTYPE_HPP_
#define NES_COMMON_INCLUDE_UTIL_REQUESTTYPE_HPP_
#include <cstdint>

namespace NES {

/**
 * @brief Represents various request types.
 *
 * Add: Add query.
 * Stop: Stop query.
 * Restart: Restart query.
 * Fail: Fail query.
 * Migrate: Migrate query.
 * Update: Update running query.
 */
enum class RequestType : uint8_t {
    AddQuery = 0,
    StopQuery,
    RestartQuery,
    FailQuery,
    MigrateQuery,
    UpdateQuery,
    AddTopologyNode,
    RemoveTopologyNode,
    MigrateTopologyNode,
    AddTopologyLink,
    RemoveTopologyLink
};

}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_REQUESTTYPE_HPP_
