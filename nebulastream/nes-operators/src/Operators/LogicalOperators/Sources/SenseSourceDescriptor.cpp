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
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <utility>

namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string udfs)
    : SourceDescriptor(std::move(schema)), udfs(std::move(udfs)) {}

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string sourceName, std::string udfs)
    : SourceDescriptor(std::move(schema), std::move(sourceName)), udfs(std::move(udfs)) {}

const std::string& SenseSourceDescriptor::getUdfs() const { return udfs; }

SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string sourceName, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(
        SenseSourceDescriptor(std::move(schema), std::move(sourceName), std::move(udfs)));
}

SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(SenseSourceDescriptor(std::move(schema), std::move(udfs)));
}

bool SenseSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<SenseSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<SenseSourceDescriptor>();
    return udfs == otherSource->getUdfs() && getSchema()->equals(otherSource->getSchema());
}

std::string SenseSourceDescriptor::toString() const { return "SenseSourceDescriptor()"; }

SourceDescriptorPtr SenseSourceDescriptor::copy() {
    auto copy = SenseSourceDescriptor::create(schema->copy(), logicalSourceName, udfs);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES
