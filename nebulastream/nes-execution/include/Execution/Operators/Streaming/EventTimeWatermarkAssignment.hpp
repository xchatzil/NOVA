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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_EVENTTIMEWATERMARKASSIGNMENT_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_EVENTTIMEWATERMARKASSIGNMENT_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;
/**
 * @brief Watermark assignment operator.
 * Determines the watermark ts according to a WatermarkStrategyDescriptor an places it in the current buffer.
 */
class EventTimeWatermarkAssignment : public ExecutableOperator {
  public:
    /**
     * @brief Creates a EventTimeWatermarkAssignment operator with a watermarkExtractionExpression expression.
     * @param TimeFunctionPtr the time function
     */
    EventTimeWatermarkAssignment(TimeFunctionPtr timeFunction);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    std::unique_ptr<TimeFunction> timeFunction;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_EVENTTIMEWATERMARKASSIGNMENT_HPP_
