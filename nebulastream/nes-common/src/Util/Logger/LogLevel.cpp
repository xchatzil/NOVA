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

#include <Util/Logger/LogLevel.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {
/**
 * @brief getLogName returns the string representation LogLevel value for a specific LogLevel value.
 * @param value LogLevel
 * @return string of value
 */
std::basic_string_view<char> getLogName(LogLevel value) { return magic_enum::enum_name(value); }
}// namespace NES
