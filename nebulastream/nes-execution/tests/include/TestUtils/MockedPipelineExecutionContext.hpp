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

#ifndef NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_MOCKEDPIPELINEEXECUTIONCONTEXT_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_MOCKEDPIPELINEEXECUTIONCONTEXT_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <set>
#include <vector>

namespace NES::Runtime::Execution {
class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    explicit MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr> handler,
                                            bool logSeenSeqChunk,
                                            BufferManagerPtr bufferManager = nullptr);
    explicit MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr> handler);
    explicit MockedPipelineExecutionContext(bool logSeenSeqChunk);
    explicit MockedPipelineExecutionContext();
    std::vector<TupleBuffer> buffers;
    std::set<SequenceData> seenSeqChunkLastChunk;
};
}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_MOCKEDPIPELINEEXECUTIONCONTEXT_HPP_
