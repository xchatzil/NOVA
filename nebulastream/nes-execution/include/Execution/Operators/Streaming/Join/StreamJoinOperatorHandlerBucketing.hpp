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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_

#include "Identifiers/Identifiers.hpp"
#include <Execution/Operators/Streaming/Join/OperatorHandlerInterfaces/JoinOperatorHandlerInterfaceBucketing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamJoinOperatorHandlerBucketing : public virtual JoinOperatorHandlerInterfaceBucketing,
                                           public virtual StreamJoinOperatorHandler {
  public:
    std::vector<StreamSlice*>* getAllWindowsToFillForTs(uint64_t ts, WorkerThreadId workerThreadId) override;
    std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice) override;
    void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) override;

  private:
    std::vector<std::vector<StreamSlice*>> windowsToFill;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLERBUCKETING_HPP_
