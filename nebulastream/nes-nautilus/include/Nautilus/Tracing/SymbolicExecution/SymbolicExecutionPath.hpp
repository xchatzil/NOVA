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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_

#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <ostream>

namespace NES::Nautilus::Tracing {

/**
 * @brief The SymbolicExecutionPath represents all tags in one specific execution.
 */
class SymbolicExecutionPath {
  public:
    void append(bool outcome);
    std::tuple<bool> operator[](uint64_t size);
    uint64_t getSize();
    friend std::ostream& operator<<(std::ostream& os, const SymbolicExecutionPath& path);
    [[maybe_unused]] std::vector<bool>& getPath();
    [[nodiscard]] const Tag* getFinalTag() const;
    void setFinalTag(const Tag* finalTag);

  private:
    std::vector<bool> path;
    const Tag* finalTag;
};

}// namespace NES::Nautilus::Tracing

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_
