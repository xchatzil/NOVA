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

#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
MQTTSinkDescriptor::MQTTSinkDescriptor(std::string&& address,
                                       std::string&& topic,
                                       std::string&& user,
                                       uint64_t maxBufferedMSGs,
                                       NES::MQTTSinkDescriptor::TimeUnits timeUnit,
                                       uint64_t messageDelay,
                                       NES::MQTTSinkDescriptor::ServiceQualities qualityOfService,
                                       bool asynchronousClient,
                                       std::string&& clientId,
                                       uint64_t numberOfOrigins)
    : SinkDescriptor(numberOfOrigins), address(std::move(address)), topic(std::move(topic)), user(std::move(user)),
      maxBufferedMSGs(maxBufferedMSGs), timeUnit(timeUnit), messageDelay(messageDelay), qualityOfService(qualityOfService),
      asynchronousClient(asynchronousClient), clientId(std::move(clientId)) {}

std::string MQTTSinkDescriptor::getAddress() const { return address; }

std::string MQTTSinkDescriptor::getClientId() const { return clientId; }

std::string MQTTSinkDescriptor::getTopic() const { return topic; }

std::string MQTTSinkDescriptor::getUser() const { return user; }

uint64_t MQTTSinkDescriptor::getMaxBufferedMSGs() const { return maxBufferedMSGs; }

MQTTSinkDescriptor::TimeUnits MQTTSinkDescriptor::getTimeUnit() const { return timeUnit; }

uint64_t MQTTSinkDescriptor::getMsgDelay() const { return messageDelay; }

MQTTSinkDescriptor::ServiceQualities MQTTSinkDescriptor::getQualityOfService() const { return qualityOfService; }

bool MQTTSinkDescriptor::getAsynchronousClient() const { return asynchronousClient; }

SinkDescriptorPtr MQTTSinkDescriptor::create(std::string&& address,
                                             std::string&& topic,
                                             std::string&& user,
                                             uint64_t maxBufferedMSGs,
                                             TimeUnits timeUnit,
                                             uint64_t messageDelay,
                                             ServiceQualities qualityOfService,
                                             bool asynchronousClient,
                                             std::string&& clientId,
                                             uint64_t numberOfOrigins) {
    return std::make_shared<MQTTSinkDescriptor>(std::move(address),
                                                std::move(topic),
                                                std::move(user),
                                                maxBufferedMSGs,
                                                timeUnit,
                                                messageDelay,
                                                qualityOfService,
                                                asynchronousClient,
                                                std::move(clientId),
                                                numberOfOrigins);
}

std::string MQTTSinkDescriptor::toString() const { return "MQTTSinkDescriptor()"; }

bool MQTTSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<MQTTSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<MQTTSinkDescriptor>();
    NES_TRACE("MQTTSinkDescriptor::equal: this: {} otherSinkDescriptor: {}", this->toString(), otherSinkDescriptor->toString());
    return address == otherSinkDescriptor->address && clientId == otherSinkDescriptor->clientId
        && topic == otherSinkDescriptor->topic && user == otherSinkDescriptor->user
        && maxBufferedMSGs == otherSinkDescriptor->maxBufferedMSGs && timeUnit == otherSinkDescriptor->timeUnit
        && messageDelay == otherSinkDescriptor->messageDelay && qualityOfService == otherSinkDescriptor->qualityOfService
        && asynchronousClient == otherSinkDescriptor->asynchronousClient;
}
}// namespace NES
