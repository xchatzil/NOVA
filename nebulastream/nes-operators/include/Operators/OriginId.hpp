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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_ORIGINID_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_ORIGINID_HPP_

#include <cstdint>

namespace NES {
/**
 * @brief this alias represent a origin.
 */
using OriginId = uint64_t;

/**
 * @brief The invalid origin id is always zero.
 */
static constexpr OriginId INVALID_ORIGIN_ID = 0;
}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_ORIGINID_HPP_
