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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_OPERATORHANDLERINTERFACES_JOINOPERATORHANDLERINTERFACEBUCKETING_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_OPERATORHANDLERINTERFACES_JOINOPERATORHANDLERINTERFACEBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <cstdint>
#include <vector>

namespace NES::Runtime::Execution::Operators {
class JoinOperatorHandlerInterfaceBucketing {
  public:
    /**
     * @brief Gets the pointer to all windows (represented by StreamSlices) that should be filled for the given timestamp.
     * If any windows does not exist for the timestamp, it will be created
     * @param ts
     * @param workerThreadId
     * @return Vector of pointer to StreamSlices
     */
    virtual std::vector<StreamSlice*>* getAllWindowsToFillForTs(uint64_t ts, WorkerThreadId workerThreadId) = 0;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_OPERATORHANDLERINTERFACES_JOINOPERATORHANDLERINTERFACEBUCKETING_HPP_
