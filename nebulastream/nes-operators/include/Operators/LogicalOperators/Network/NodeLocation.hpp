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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NODELOCATION_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NODELOCATION_HPP_

#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/core.h>

namespace NES::Network {

/**
 * @brief This is a network location of a nes node. It contains a node id, an ip address, and port for data transfer.
 */
class NodeLocation {
  public:
    explicit NodeLocation() = default;

    explicit NodeLocation(WorkerId workerId, std::string hostname, uint32_t port)
        : workerId(workerId), hostname(std::move(hostname)), port(port) {
        NES_ASSERT2_FMT(!this->hostname.empty(), "Empty hostname passed on " << workerId);
    }

    NodeLocation(const NodeLocation& other) : workerId(other.workerId), hostname(other.hostname), port(other.port) {}

    NodeLocation& operator=(const NodeLocation& other) {
        workerId = other.workerId;
        hostname = other.hostname;
        port = other.port;
        return *this;
    }

    [[nodiscard]] constexpr auto operator!() const noexcept -> bool {
        return hostname.empty() && port == 0 && workerId == INVALID_WORKER_NODE_ID;
    }

    /**
     * @brief Returns the zmq uri for connection
     * @return the zmq uri for connection
     */
    [[nodiscard]] std::string createZmqURI() const { return fmt::format("tcp://{}:{}", hostname, std::to_string(port)); }

    /**
     * @brief Return the node id
     * @return the node id
     */
    [[nodiscard]] WorkerId getNodeId() const { return workerId; }

    /**
     * @brief Returns the hostname
     * @return the hostname
     */
    [[nodiscard]] const std::string& getHostname() const { return hostname; }

    /**
     * @brief Returns the port
     * @return the port
     */
    [[nodiscard]] uint32_t getPort() const { return port; }

    /**
     * @brief The equals operator for the NodeLocation.
     * @param lhs left node location
     * @param rhs right node location
     * @return true, if they are equal, else false
     */
    friend bool operator==(const NodeLocation& lhs, const NodeLocation& rhs) {
        return lhs.workerId == rhs.workerId && lhs.hostname == rhs.hostname && lhs.port == rhs.port;
    }

  private:
    WorkerId workerId = INVALID_WORKER_NODE_ID;
    std::string hostname;
    uint32_t port;
};
}// namespace NES::Network
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NODELOCATION_HPP_
