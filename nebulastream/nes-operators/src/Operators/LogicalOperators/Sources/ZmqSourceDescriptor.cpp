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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <utility>

namespace NES {

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(ZmqSourceDescriptor(std::move(schema), std::move(host), port));
}

SourceDescriptorPtr ZmqSourceDescriptor::create(SchemaPtr schema, std::string sourceName, std::string host, uint16_t port) {
    return std::make_shared<ZmqSourceDescriptor>(
        ZmqSourceDescriptor(std::move(schema), std::move(sourceName), std::move(host), port));
}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port)
    : SourceDescriptor(std::move(schema)), host(std::move(host)), port(port) {}

ZmqSourceDescriptor::ZmqSourceDescriptor(SchemaPtr schema, std::string sourceName, std::string host, uint16_t port)
    : SourceDescriptor(std::move(schema), std::move(sourceName)), host(std::move(host)), port(port) {}

const std::string& ZmqSourceDescriptor::getHost() const { return host; }
uint16_t ZmqSourceDescriptor::getPort() const { return port; }

bool ZmqSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<ZmqSourceDescriptor>()) {
        return false;
    }
    auto otherZMQSource = other->as<ZmqSourceDescriptor>();
    return host == otherZMQSource->getHost() && port == otherZMQSource->getPort() && getSchema()->equals(other->getSchema());
}

std::string ZmqSourceDescriptor::toString() const { return "ZmqSourceDescriptor()"; }

void ZmqSourceDescriptor::setPort(uint16_t newPort) { this->port = newPort; }

SourceDescriptorPtr ZmqSourceDescriptor::copy() {
    auto copy = ZmqSourceDescriptor::create(schema->copy(), logicalSourceName, host, port);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES
