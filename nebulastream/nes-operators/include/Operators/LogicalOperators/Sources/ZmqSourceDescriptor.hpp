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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq source
 */
class ZmqSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string host, uint16_t port);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string sourceName, std::string host, uint16_t port);

    /**
     * @brief Get zmq address name
     */
    const std::string& getHost() const;

    /**
     * @brief Get zmq port number
     */
    uint16_t getPort() const;

    /**
     * Set the zmq port information
     * @param port : zmq port number
     */
    void setPort(uint16_t port);

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;
    std::string toString() const override;
    SourceDescriptorPtr copy() override;

  private:
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port);
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string sourceName, std::string host, uint16_t port);

    std::string host;
    uint16_t port;
};

using ZmqSourceDescriptorPtr = std::shared_ptr<ZmqSourceDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
