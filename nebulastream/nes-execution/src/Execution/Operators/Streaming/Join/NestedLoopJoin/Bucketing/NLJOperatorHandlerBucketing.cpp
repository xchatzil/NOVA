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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Bucketing/NLJOperatorHandlerBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandlerBucketing::NLJOperatorHandlerBucketing(const std::vector<OriginId>& inputOrigins,
                                                         const OriginId outputOriginId,
                                                         const uint64_t windowSize,
                                                         const uint64_t windowSlide,
                                                         const SchemaPtr& leftSchema,
                                                         const SchemaPtr& rightSchema,
                                                         const uint64_t pageSizeLeft,
                                                         const uint64_t pageSizeRight)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, leftSchema, rightSchema),
      NLJOperatorHandler(inputOrigins,
                         outputOriginId,
                         windowSize,
                         windowSlide,
                         leftSchema,
                         rightSchema,
                         pageSizeLeft,
                         pageSizeRight) {}

NLJOperatorHandlerPtr NLJOperatorHandlerBucketing::create(const std::vector<OriginId>& inputOrigins,
                                                          const OriginId outputOriginId,
                                                          const uint64_t windowSize,
                                                          const uint64_t windowSlide,
                                                          const SchemaPtr& leftSchema,
                                                          const SchemaPtr& rightSchema,
                                                          const uint64_t pageSizeLeft,
                                                          const uint64_t pageSizeRight) {
    return std::make_shared<NLJOperatorHandlerBucketing>(inputOrigins,
                                                         outputOriginId,
                                                         windowSize,
                                                         windowSlide,
                                                         leftSchema,
                                                         rightSchema,
                                                         pageSizeLeft,
                                                         pageSizeRight);
}
}// namespace NES::Runtime::Execution::Operators
