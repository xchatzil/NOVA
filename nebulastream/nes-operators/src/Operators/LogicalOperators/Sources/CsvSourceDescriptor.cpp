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
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>

namespace NES {

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema,
                                         CSVSourceTypePtr sourceConfig,
                                         const std::string& logicalSourceName,
                                         const std::string& physicalSourceName)
    : SourceDescriptor(std::move(schema), logicalSourceName, physicalSourceName), csvSourceType(std::move(sourceConfig)) {}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema,
                                                CSVSourceTypePtr csvSourceType,
                                                const std::string& logicalSourceName,
                                                const std::string& physicalSourceName) {
    return std::make_shared<CsvSourceDescriptor>(
        CsvSourceDescriptor(std::move(schema), std::move(csvSourceType), logicalSourceName, physicalSourceName));
}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema, CSVSourceTypePtr csvSourceType) {
    return std::make_shared<CsvSourceDescriptor>(CsvSourceDescriptor(std::move(schema), std::move(csvSourceType), "", ""));
}

CSVSourceTypePtr CsvSourceDescriptor::getSourceConfig() const { return csvSourceType; }

bool CsvSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<CsvSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<CsvSourceDescriptor>();
    return csvSourceType->equal(otherSource->getSourceConfig());
}

std::string CsvSourceDescriptor::toString() const { return "CsvSourceDescriptor(" + csvSourceType->toString() + ")"; }

SourceDescriptorPtr CsvSourceDescriptor::copy() {
    auto copy = CsvSourceDescriptor::create(schema->copy(), csvSourceType, logicalSourceName, physicalSourceName);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES
