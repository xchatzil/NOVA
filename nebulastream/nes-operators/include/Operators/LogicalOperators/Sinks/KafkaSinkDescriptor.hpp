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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <string>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical kafka sink
 */
class KafkaSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new Kafka sink.
     * @param topic kafka topic name
     * @param brokers kafka broker list
     * @param timeout Kafka producer timeout
     * @return descriptor for kafka sink
     */
    static SinkDescriptorPtr
    create(const std::string& sinkFormat, const std::string& topic, const std::string& brokers, uint64_t timeout);

    /**
     * @brief Get Kafka topic where data is to be written
     */
    const std::string& getTopic() const;

    /**
     * @brief List of comma separated kafka brokers
     */
    const std::string& getBrokers() const;

    /**
     * @brief Kafka connection Timeout
     */
    uint64_t getTimeout() const;

    std::string toString() const override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;
    std::string getSinkFormatAsString() const;

  private:
    explicit KafkaSinkDescriptor(const std::string& sinkFormat,
                                 const std::string& topic,
                                 const std::string& brokers,
                                 uint64_t timeout);
    std::string sinkFormat;
    std::string topic;
    std::string brokers;
    uint64_t timeout;
};

typedef std::shared_ptr<KafkaSinkDescriptor> KafkaSinkDescriptorPtr;
}// namespace NES
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_KAFKASINKDESCRIPTOR_HPP_
