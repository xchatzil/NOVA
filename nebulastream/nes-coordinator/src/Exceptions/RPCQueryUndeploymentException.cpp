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
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <utility>
namespace NES::Exceptions {
RPCQueryUndeploymentException::RPCQueryUndeploymentException(const std::string& message,
                                                             std::vector<WorkerId> failedRpcWorkerIds,
                                                             RpcClientMode mode)
    : RequestExecutionException(message), failedWorkerIds(std::move(failedRpcWorkerIds)), mode(mode) {}

const char* RPCQueryUndeploymentException::what() const noexcept { return message.c_str(); }

std::vector<WorkerId> RPCQueryUndeploymentException::getFailedWorkerIds() { return failedWorkerIds; }

RpcClientMode RPCQueryUndeploymentException::getMode() { return mode; }
}// namespace NES::Exceptions
