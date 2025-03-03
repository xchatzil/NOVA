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

#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
namespace NES {

TaskExecutionException::TaskExecutionException(Runtime::Execution::SuccessorExecutablePipeline pipeline, std::string&& message)
    : Exceptions::RuntimeException(message), pipeline(pipeline) {}

Runtime::Execution::SuccessorExecutablePipeline TaskExecutionException::getExecutable() const { return pipeline; }

}// namespace NES
