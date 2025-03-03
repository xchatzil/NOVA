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
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <utility>

namespace NES {

SinkDescriptorPtr ZmqSinkDescriptor::create(std::string host, uint16_t port, bool internal, uint64_t numberOfOrigins) {
    return std::make_shared<ZmqSinkDescriptor>(ZmqSinkDescriptor(std::move(host), port, internal, numberOfOrigins));
}

ZmqSinkDescriptor::ZmqSinkDescriptor(std::string host, uint16_t port, bool internal, uint64_t numberOfOrigins)
    : SinkDescriptor(numberOfOrigins), host(std::move(host)), port(port), internal(internal) {}

const std::string& ZmqSinkDescriptor::getHost() const { return host; }
uint16_t ZmqSinkDescriptor::getPort() const { return port; }

bool ZmqSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<ZmqSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<ZmqSinkDescriptor>();
    return host == otherSinkDescriptor->host && port == otherSinkDescriptor->port;
}

std::string ZmqSinkDescriptor::toString() const { return "ZmqSinkDescriptor()"; }

void ZmqSinkDescriptor::setPort(uint16_t newPort) { this->port = newPort; }

bool ZmqSinkDescriptor::isInternal() const { return internal; }

void ZmqSinkDescriptor::setInternal(bool newInternal) { ZmqSinkDescriptor::internal = newInternal; }

}// namespace NES
