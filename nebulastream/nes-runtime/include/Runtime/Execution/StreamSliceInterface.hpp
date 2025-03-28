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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_STREAMSLICEINTERFACE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_STREAMSLICEINTERFACE_HPP_
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstdint>
#include <span>

namespace NES::Runtime::Execution {
/**
 * @brief Basic Slice Interface with necessary methods and variables for migration
 */
class StreamSliceInterface {
  public:
    /**
     * @brief Getter for the start ts of the slice
     * @return uint64_t
     */
    [[nodiscard]] virtual uint64_t getSliceStart() const = 0;

    /**
     * @brief Getter for the end ts of the slice
     * @return uint64_t
     */
    [[nodiscard]] virtual uint64_t getSliceEnd() const = 0;

    /**
     * @brief Gets stored state as vector of tuple buffers
     * @param std::shared_ptr<BufferManager>&
     * @return list of pages that store records and metadata
     */
    virtual std::vector<Runtime::TupleBuffer> serialize(std::shared_ptr<BufferManager>&) = 0;

    virtual ~StreamSliceInterface() = default;
};
}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_STREAMSLICEINTERFACE_HPP_
