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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_DUMPMODE_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_DUMPMODE_HPP_

#include <cstdint>

namespace NES::QueryCompilation {
enum class DumpMode : uint8_t {
    // Disables all dumping
    NONE,
    // Dumps intermediate representations to console, std:out
    CONSOLE,
    // Dumps intermediate representations to file
    FILE,
    // Dumps intermediate representations to console and file
    FILE_AND_CONSOLE
};
}
#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_DUMPMODE_HPP_
