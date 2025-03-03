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

#ifndef NES_COMPILER_INCLUDE_COMPILER_COMPILERFLAGS_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_COMPILERFLAGS_HPP_

#include <string>
#include <unordered_set>

namespace NES::Compiler {

/**
 * @brief This class abstracts the collection of compiler flags. Each collected flag is unique.
 * Flags can be added, retrieved and two sets of CompilerFlags can also be merged.
 */
class CompilerFlags {
  public:
    CompilerFlags() = default;

    /**
     * @brief Get all of the compiler flags.
     * @return A std::unordered_set<std::string> of all the compiler flags.
     */
    [[nodiscard]] std::unordered_set<std::string> getFlags() const;

    /**
     * @brief Add a new compiler flag to the collection.
     * @param flag the new compiler flag.
     */
    void addFlag(const std::string& flag);

    /**
     * @brief Merge the compiler flags of another CompilerFlags with the flags of this instance.
     * @param flags the other CompilerFlags instance
     */
    void mergeFlags(const CompilerFlags& flags);

  private:
    std::unordered_set<std::string> compilerFlags;
};

}// namespace NES::Compiler

#endif// NES_COMPILER_INCLUDE_COMPILER_COMPILERFLAGS_HPP_
