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

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <string>
namespace NES::Runtime::Execution {

uint32_t ExecutablePipelineStage::setup(PipelineExecutionContext&) { return 0; }

uint32_t ExecutablePipelineStage::start(PipelineExecutionContext&) { return 0; }

uint32_t ExecutablePipelineStage::open(PipelineExecutionContext&, WorkerContext&) { return 0; }

uint32_t ExecutablePipelineStage::close(PipelineExecutionContext&, WorkerContext&) { return 0; }

uint32_t ExecutablePipelineStage::stop(PipelineExecutionContext&) { return 0; }

std::string ExecutablePipelineStage::getCodeAsString() { return ""; };

}// namespace NES::Runtime::Execution
