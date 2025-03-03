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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_EVENTBUFFERMESSAGE_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_EVENTBUFFERMESSAGE_HPP_

#include <Network/NetworkMessage.hpp>

namespace NES::Network::Messages {

/**
 * @brief This a payload message with an event
 */
class EventBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::EventBuffer;

    explicit inline EventBufferMessage(Runtime::EventType eventType, uint32_t payloadSize) noexcept
        : eventType(eventType), payloadSize(payloadSize) {}

    Runtime::EventType const eventType;
    uint32_t const payloadSize;
};

}// namespace NES::Network::Messages
#endif// NES_RUNTIME_INCLUDE_NETWORK_EVENTBUFFERMESSAGE_HPP_
