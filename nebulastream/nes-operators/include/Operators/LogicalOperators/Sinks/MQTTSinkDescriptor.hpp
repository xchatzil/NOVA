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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <string>

namespace NES {
/**
 * @brief Descriptor defining properties used for creating a physical MQTT sink
 */
class MQTTSinkDescriptor : public SinkDescriptor {
  public:
    enum class TimeUnits : uint8_t { nanoseconds, milliseconds, seconds };
    enum class ServiceQualities : uint8_t {
        atMostOnce,
        atLeastOnce,
        exactlyOnce
    };//cleanSession requires atLeastOnce or exactlyOnce
    /**
     * @brief Creates the MQTT sink description
     * @param address: address name of MQTT broker
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param messageDelay: time before next message is sent by client to broker
     * @param qualityOfService: either 'at most once' or 'at least once'. QOS > 0 required for a non-clean (persistent) session.
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @param clientId: client ID for MQTT client. If non is given, the operatorID is used automatically (see 'ConvertLogicalToPhysicalSink.cpp).
     * @param numberOfOrigins: number of origins of a given query
     * @return descriptor for MQTT sink
     */
    static SinkDescriptorPtr create(std::string&& address,
                                    std::string&& topic,
                                    std::string&& user = "user",
                                    uint64_t maxBufferedMSGs = 1000,
                                    TimeUnits timeUnit = TimeUnits::milliseconds,
                                    uint64_t messageDelay = 0,
                                    ServiceQualities qualityOfService = ServiceQualities::atLeastOnce,
                                    bool asynchronousClient = true,
                                    std::string&& clientId = "",
                                    uint64_t numberOfOrigins = 1);

    /**
     * @brief get address information from a MQTT sink client
     * @return address of MQTT broker
     */
    std::string getAddress() const;

    /**
     * @brief get clientId information from a MQTT sink client
     * @return id used by client
     */
    std::string getClientId() const;

    /**
     * @brief get topic information from a MQTT sink client
     * @return topic to which MQTT client sends messages
     */
    std::string getTopic() const;

    /**
     * @brief get user name for a MQTT sink client
     * @return user name used by MQTT client
     */
    std::string getUser() const;

    /**
     * @brief get the number of MSGs that can maximally be buffered (default is 60)
     * @return number of messages that can maximally be buffered
     */
    uint64_t getMaxBufferedMSGs() const;

    /**
     * @brief get the user chosen time unit (default is milliseconds)
     * @return time unit chosen for the message delay
     */
    TimeUnits getTimeUnit() const;

    /**
     * @brief get the user chosen delay between two sent messages (default is 500)
     * @return length of the message delay
     */
    uint64_t getMsgDelay() const;

    /**
     * @brief get the value for the current quality of service
     * @return quality of service value
     */
    ServiceQualities getQualityOfService() const;

    /**
     * @brief get bool that indicates whether the client is asynchronous or synchronous (default is true)
     * @return true if client is asynchronous, else false
     */
    bool getAsynchronousClient() const;

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    /**
     * @brief Creates the MQTT sink descriptor
     * @param address address name of MQTT broker
     * @param topic mqtt topic to write date to
     * @param user user identification of client
     * @param maxBufferedMSGs maximum number of messages that can be buffered by the client before disconnecting
     * @param timeUnit time unit chosen by client user foe message delay
     * @param messageDelay time before next message is sent by client to broker
     * @param qualityOfService either 'at most once' or 'at least once'. QOS > 0 required for a non-clean (persistent) session.
     * @param asynchronousClient determine whether client is async- or synchronous
     * @param clientId client ID for MQTT client
     * @param numberOfOrigins number of origins of a given query
     */
    explicit MQTTSinkDescriptor(std::string&& address,
                                std::string&& topic,
                                std::string&& user,
                                uint64_t maxBufferedMSGs,
                                TimeUnits timeUnit,
                                uint64_t messageDelay,
                                ServiceQualities qualityOfService,
                                bool asynchronousClient,
                                std::string&& clientId,
                                uint64_t numberOfOrigins);

  private:
    std::string address;
    std::string topic;
    std::string user;
    uint64_t maxBufferedMSGs;
    TimeUnits timeUnit;
    uint64_t messageDelay;
    ServiceQualities qualityOfService;
    bool asynchronousClient;
    std::string clientId;
};

using MQTTSinkDescriptorPtr = std::shared_ptr<MQTTSinkDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
