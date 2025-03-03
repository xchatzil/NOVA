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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_

#include <Network/NetworkMessage.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Network {
/**
 * @brief Listener for network stack events
 */
class ExchangeProtocolListener {
  public:
    virtual ~ExchangeProtocolListener() = default;

    /**
     * @brief This is called on every event buffer received by the network stack.
     * @param NesPartition partition that receives the event
     * @param Runtime::BaseEvent& ref to the actual event
     */
    virtual void onEvent(NesPartition, Runtime::BaseEvent&) = 0;

    /**
     * @brief This is called on every data buffer that the network stack receives
     * for a specific nes partition.
     * @param NesPartition partition that receives the data buffer
     * @param Runtime::TupleBuffer& ref to the actual data buffer
     */
    virtual void onDataBuffer(NesPartition, Runtime::TupleBuffer&) = 0;
    /**
     * @brief this is called once a nes partition receives an end of stream message.
     * @param Messages::EndOfStreamMessage eos descriptor
     */
    virtual void onEndOfStream(Messages::EndOfStreamMessage) = 0;
    /**
     * @brief this is called on the server side as soon as an error is raised.
     * @param Messages::ErrorMessage error message descriptor
     */
    virtual void onServerError(Messages::ErrorMessage) = 0;
    /**
     * @brief This is called on the channel side as soon as an error is raised.
     * @param Messages::ErrorMessage error message descriptor
     */
    virtual void onChannelError(Messages::ErrorMessage) = 0;
};
}// namespace NES::Network
#endif// NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
