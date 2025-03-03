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

#ifndef NES_WORKER_INCLUDE_GRPC_CALLDATA_HPP_
#define NES_WORKER_INCLUDE_GRPC_CALLDATA_HPP_

#include <stdint.h>

namespace grpc {
class ServerCompletionQueue;
}

namespace NES {
class WorkerRPCServer;
/**
 * @brief This is taken from https://github.com/grpc/grpc/tree/master/examples/cpp/helloworld
 *  Take in the "service" instance (in this case representing an asynchronous
 * server) and the completion queue "completionQueue" used for asynchronous communication
 * with the gRPC Runtime.
 */
class CallData {
  public:
    /**
     * @brief Constructor for the Call Data
     * @param service server to listen on
     * @param cq queue to listen on
     */
    explicit CallData(WorkerRPCServer& service);

    /**
    * @brief Run method to process the call data through it different stages
    */
    void proceed();

  private:
    // The means of communication with the gRPC Runtime for an asynchronous
    // server.
    WorkerRPCServer& service;

    // Let's implement a tiny state machine with the following states.
    enum class CallStatus : uint8_t { CREATE, PROCESS, FINISH };
    CallStatus status = CallStatus::CREATE;// The current serving state.
};

}// namespace NES
#endif// NES_WORKER_INCLUDE_GRPC_CALLDATA_HPP_
