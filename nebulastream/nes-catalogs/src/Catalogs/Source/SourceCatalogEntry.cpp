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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Catalogs::Source {

SourceCatalogEntry::SourceCatalogEntry(PhysicalSourcePtr physicalSource, LogicalSourcePtr logicalSource, WorkerId topologyNodeId)
    : physicalSource(std::move(physicalSource)), logicalSource(std::move(logicalSource)), topologyNodeId(topologyNodeId) {}

SourceCatalogEntryPtr
SourceCatalogEntry::create(PhysicalSourcePtr physicalSource, LogicalSourcePtr logicalSource, WorkerId topologyNodeId) {
    return std::make_shared<SourceCatalogEntry>(SourceCatalogEntry(physicalSource, logicalSource, topologyNodeId));
}

const PhysicalSourcePtr& SourceCatalogEntry::getPhysicalSource() const { return physicalSource; }

const LogicalSourcePtr& SourceCatalogEntry::getLogicalSource() const { return logicalSource; }

WorkerId SourceCatalogEntry::getTopologyNodeId() const { return topologyNodeId; }

std::string SourceCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalSource=" << physicalSource->getPhysicalSourceName()
       << " logicalSource=" << logicalSource->getLogicalSourceName() << " on node=" << topologyNodeId;
    return ss.str();
}

nlohmann::json SourceCatalogEntry::toJson() {
    nlohmann::json sourceEntryJson{};

    sourceEntryJson["physicalSourceName"] = physicalSource->getPhysicalSourceName();
    sourceEntryJson["logicalSourceName"] = logicalSource->getLogicalSourceName();
    sourceEntryJson["physicalSourceType"] = physicalSource->getPhysicalSourceTypeName();
    sourceEntryJson["nodeId"] = topologyNodeId.getRawValue();

    return sourceEntryJson;
}

}// namespace NES::Catalogs::Source
