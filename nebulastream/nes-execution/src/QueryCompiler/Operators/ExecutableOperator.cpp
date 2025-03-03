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
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <utility>

namespace NES::QueryCompilation {

ExecutableOperator::ExecutableOperator(OperatorId id,
                                       Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage,
                                       std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers)
    : Operator(id), UnaryOperator(id), executablePipelineStage(std::move(executablePipelineStage)),
      operatorHandlers(std::move(operatorHandlers)) {}

OperatorPtr ExecutableOperator::create(Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage,
                                       std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers) {
    return std::make_shared<ExecutableOperator>(
        ExecutableOperator(getNextOperatorId(), std::move(executablePipelineStage), std::move(operatorHandlers)));
}

Runtime::Execution::ExecutablePipelineStagePtr ExecutableOperator::getExecutablePipelineStage() {
    return executablePipelineStage;
}

std::vector<Runtime::Execution::OperatorHandlerPtr> ExecutableOperator::getOperatorHandlers() { return operatorHandlers; }

std::string ExecutableOperator::toString() const { return "ExecutableOperator"; }

OperatorPtr ExecutableOperator::copy() { return create(executablePipelineStage, operatorHandlers); }

}// namespace NES::QueryCompilation
