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
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <utility>

namespace NES {

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string filePath)
    : SourceDescriptor(std::move(schema)), filePath(std::move(filePath)) {}

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string sourceName, std::string filePath)
    : SourceDescriptor(std::move(schema), std::move(sourceName)), filePath(std::move(filePath)) {}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string filePath) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(std::move(schema), std::move(filePath)));
}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string sourceName, std::string filePath) {
    return std::make_shared<BinarySourceDescriptor>(
        BinarySourceDescriptor(std::move(schema), std::move(sourceName), std::move(filePath)));
}

const std::string& BinarySourceDescriptor::getFilePath() const { return filePath; }

bool BinarySourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<BinarySourceDescriptor>()) {
        return false;
    }
    auto otherDefaultSource = other->as<BinarySourceDescriptor>();
    return filePath == otherDefaultSource->getFilePath() && getSchema()->equals(otherDefaultSource->getSchema());
}

std::string BinarySourceDescriptor::toString() const { return "BinarySourceDescriptor(" + filePath + ")"; }

SourceDescriptorPtr BinarySourceDescriptor::copy() {
    auto copy = BinarySourceDescriptor::create(schema->copy(), filePath);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}
}// namespace NES
