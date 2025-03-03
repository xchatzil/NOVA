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
#ifndef NES_PLUGINS_CUDA_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_UNVECTORIZE_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_UNVECTORIZE_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Vectorization/VectorizableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Unvectorize operator which reverts the execution mode from vectorized to tuple-at-a-time.
 * The child operator is executed on each tuple that is retrieved from an incoming tuple buffer.
 */
class Unvectorize : public VectorizableOperator {
  public:
    /**
     * @brief Constructor.
     * @param memoryProvider the memory layout that describes the tuple buffer.
     * @param projections the projection vector.
     */
    Unvectorize(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
                std::vector<Record::RecordFieldIdentifier> projections = {});

    void execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    std::vector<Record::RecordFieldIdentifier> projections;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_PLUGINS_CUDA_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_UNVECTORIZE_HPP_
