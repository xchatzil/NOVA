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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
namespace NES::Runtime::Execution {
/**
 * @brief Represents the query status.
 */
enum class ExecutableQueryPlanStatus : uint8_t {
    Created,
    Deployed,// Created->Deployed when calling setup()
    Running, // Deployed->Running when calling start()
    Finished,// Running->Finished when all data sources soft stop
    Stopped, // Running->Stopped when calling stop() and in Running state
    ErrorState,
    Invalid
};
}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
