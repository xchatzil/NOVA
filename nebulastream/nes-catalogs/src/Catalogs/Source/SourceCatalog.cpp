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

#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <SerializableSchema.pb.h>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/adl_serializer.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES::Catalogs::Source {
SourceCatalog::SourceCatalog() {
    NES_DEBUG("SourceCatalog: construct source catalog");
    addDefaultSources();
    NES_DEBUG("SourceCatalog: construct source catalog successfully");
}

void SourceCatalog::addDefaultSources() {
    NES_DEBUG("Sourcecatalog addDefaultSources");
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    bool success = addLogicalSource("default_logical", schema);
    if (!success) {
        NES_ERROR("SourceCatalog::addDefaultSources: error while add default_logical");
        throw Exceptions::RuntimeException("Error while addDefaultSources SourceCatalog");
    }
}

bool SourceCatalog::addLogicalSource(const std::string& logicalSourceName, SchemaPtr schema) {
    //check if source already exist
    NES_DEBUG("SourceCatalog: Check if logical source {} already exist.", logicalSourceName);

    if (!containsLogicalSource(logicalSourceName)) {
        NES_DEBUG("SourceCatalog: add logical source {}", logicalSourceName);
        logicalSourceNameToSchemaMapping[logicalSourceName] = std::move(schema);
        return true;
    }
    NES_ERROR("SourceCatalog: logical source {} already exists", logicalSourceName);
    return false;
}

bool SourceCatalog::removeLogicalSource(const std::string& logicalSourceName) {
    NES_DEBUG("SourceCatalog: search for logical source in removeLogicalSource() {}", logicalSourceName);

    //if logical source does not exist
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR("SourceCatalog: logical source {} already exists", logicalSourceName);
        return false;
    }
    NES_DEBUG("SourceCatalog: remove logical source {}", logicalSourceName);
    if (logicalToPhysicalSourceMapping[logicalSourceName].size() != 0) {
        NES_DEBUG("SourceCatalog: cannot remove {} because there are physical entries for this source", logicalSourceName);
        return false;
    }
    uint64_t cnt = logicalSourceNameToSchemaMapping.erase(logicalSourceName);
    NES_DEBUG("SourceCatalog: removed {} copies of the source", cnt);
    NES_ASSERT(!containsLogicalSource(logicalSourceName), "log source should not exist");
    return true;
}

bool SourceCatalog::addPhysicalSource(const std::string& logicalSourceName, const SourceCatalogEntryPtr& sourceCatalogEntry) {
    NES_DEBUG("SourceCatalog: search for logical source in addPhysicalSource() {}", logicalSourceName);

    // check if logical source exists
    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR("SourceCatalog: logical source {} does not exist when inserting physical source {} ",
                  logicalSourceName,
                  sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());
        return false;
    } else {
        NES_DEBUG("SourceCatalog: logical source {} exists try to add physical source {}",
                  logicalSourceName,
                  sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());

        //get current physical source for this logical source
        std::vector<SourceCatalogEntryPtr> existingSCEs = logicalToPhysicalSourceMapping[logicalSourceName];

        //check if physical source does not exist yet
        for (const SourceCatalogEntryPtr& existingSCE : existingSCEs) {
            if (existingSCE->getPhysicalSource()->getPhysicalSourceName()
                    == sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName()
                && existingSCE->getTopologyNodeId() == sourceCatalogEntry->getTopologyNodeId()) {
                NES_ERROR("SourceCatalog: node with id={} name={} already exists",
                          sourceCatalogEntry->getTopologyNodeId(),
                          sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());
                return false;
            }
        }
    }

    NES_DEBUG("SourceCatalog: physical source  {} does not exist, try to add",
              sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());

    //if first one
    if (testIfLogicalSourceExistsInLogicalToPhysicalMapping(logicalSourceName)) {
        NES_DEBUG("SourceCatalog: Logical source already exists, add new physical entry");
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(sourceCatalogEntry);
    } else {
        NES_DEBUG("SourceCatalog: Logical source does not exist, create new item");
        logicalToPhysicalSourceMapping.insert(
            std::pair<std::string, std::vector<SourceCatalogEntryPtr>>(logicalSourceName, std::vector<SourceCatalogEntryPtr>()));
        logicalToPhysicalSourceMapping[logicalSourceName].push_back(sourceCatalogEntry);
    }

    NES_DEBUG("SourceCatalog: physical source with id={} successful added", sourceCatalogEntry->getTopologyNodeId());
    return true;
}

bool SourceCatalog::removePhysicalSource(const std::string& logicalSourceName,
                                         const std::string& physicalSourceName,
                                         WorkerId topologyNodeId) {
    NES_DEBUG("SourceCatalog: search for logical source in removePhysicalSource() {}", logicalSourceName);

    // check if logical source exists
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        NES_ERROR("SourceCatalog: logical source {} does not exist when trying to remove physical source with hashId {}",
                  logicalSourceName,
                  topologyNodeId);
        return false;
    }
    NES_DEBUG("SourceCatalog: logical source {} exists try to remove physical source{} from node {}",
              logicalSourceName,
              physicalSourceName,
              topologyNodeId);
    for (auto entry = logicalToPhysicalSourceMapping[logicalSourceName].cbegin();
         entry != logicalToPhysicalSourceMapping[logicalSourceName].cend();
         entry++) {
        NES_DEBUG("test node id={} phyStr={}",
                  entry->get()->getTopologyNodeId(),
                  entry->get()->getPhysicalSource()->getPhysicalSourceName());
        NES_DEBUG("test to be deleted id={} phyStr={}", topologyNodeId, physicalSourceName);
        if (entry->get()->getPhysicalSource()->getPhysicalSourceName() == physicalSourceName) {
            NES_DEBUG("SourceCatalog: node with name={} exists try match hashId {}", physicalSourceName, topologyNodeId);

            if (entry->get()->getTopologyNodeId() == topologyNodeId) {
                NES_DEBUG("SourceCatalog: node with id={} name={} exists try to erase", topologyNodeId, physicalSourceName);
                logicalToPhysicalSourceMapping[logicalSourceName].erase(entry);
                NES_DEBUG("SourceCatalog: number of entries afterwards {}",
                          logicalToPhysicalSourceMapping[logicalSourceName].size());
                return true;
            }
        }
    }
    NES_DEBUG("SourceCatalog: physical source {} does not exist on node with id {} and with logicalSourceName {}",
              physicalSourceName,
              topologyNodeId,
              logicalSourceName);

    NES_DEBUG("SourceCatalog: physical source {} does not exist on node with id {}", physicalSourceName, topologyNodeId);
    return false;
}

size_t SourceCatalog::removeAllPhysicalSourcesByWorker(WorkerId workerId) {
    size_t removedElements = 0;

    for (auto& [logicalSource, physicalSources] : logicalToPhysicalSourceMapping) {
        std::erase_if(physicalSources, [workerId, &removedElements](const auto& mappingEntry) {
            removedElements += mappingEntry->getTopologyNodeId() == workerId;
            return mappingEntry->getTopologyNodeId() == workerId;
        });
    }

    return removedElements;
}

SchemaPtr SourceCatalog::getSchemaForLogicalSource(const std::string& logicalSourceName) {
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) == logicalSourceNameToSchemaMapping.end()) {
        throw Exceptions::LogicalSourceNotFoundException("SourceCatalog: No schema found for logical source "
                                                         + logicalSourceName);
    }
    return logicalSourceNameToSchemaMapping[logicalSourceName];
}

LogicalSourcePtr SourceCatalog::getLogicalSource(const std::string& logicalSourceName) {
    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR("SourceCatalog::getLogicalSource: source does not exist {}", logicalSourceName);
        return nullptr;
    }
    return LogicalSource::create(logicalSourceName, logicalSourceNameToSchemaMapping[logicalSourceName]);
}

LogicalSourcePtr SourceCatalog::getLogicalSourceOrThrowException(const std::string& logicalSourceName) {
    if (logicalSourceNameToSchemaMapping.find(logicalSourceName) != logicalSourceNameToSchemaMapping.end()) {
        return LogicalSource::create(logicalSourceName, logicalSourceNameToSchemaMapping[logicalSourceName]);
    }
    NES_ERROR("SourceCatalog::getLogicalSourceOrThrowException: source does not exist {}", logicalSourceName);
    throw Exceptions::RuntimeException("Required source does not exist " + logicalSourceName);
}

bool SourceCatalog::containsLogicalSource(const std::string& logicalSourceName) {
    return logicalSourceNameToSchemaMapping.contains(logicalSourceName);
}

bool SourceCatalog::testIfLogicalSourceExistsInLogicalToPhysicalMapping(const std::string& logicalSourceName) {
    return logicalToPhysicalSourceMapping.contains(logicalSourceName);
}

std::vector<WorkerId> SourceCatalog::getSourceNodesForLogicalSource(const std::string& logicalSourceName) {
    std::vector<WorkerId> listOfSourceNodes;

    //get current physical source for this logical source
    std::vector<SourceCatalogEntryPtr> physicalSources = logicalToPhysicalSourceMapping[logicalSourceName];

    if (physicalSources.empty()) {
        return listOfSourceNodes;
    }

    for (const SourceCatalogEntryPtr& entry : physicalSources) {
        listOfSourceNodes.push_back(entry->getTopologyNodeId());
    }

    return listOfSourceNodes;
}

bool SourceCatalog::reset() {
    NES_DEBUG("SourceCatalog: reset Source Catalog");
    logicalSourceNameToSchemaMapping.clear();
    logicalToPhysicalSourceMapping.clear();

    addDefaultSources();
    NES_DEBUG("SourceCatalog: reset Source Catalog completed");
    return true;
}

std::string SourceCatalog::getPhysicalSourceAndSchemaAsString() {
    std::stringstream ss;
    for (const auto& entry : logicalToPhysicalSourceMapping) {
        ss << "source name=" << entry.first << " with " << entry.second.size() << " elements:";
        for (const SourceCatalogEntryPtr& sce : entry.second) {
            ss << sce->toString();
        }
        ss << std::endl;
    }
    return ss.str();
}

std::vector<SourceCatalogEntryPtr> SourceCatalog::getPhysicalSources(const std::string& logicalSourceName) {
    if (!logicalToPhysicalSourceMapping.contains(logicalSourceName)) {
        NES_ERROR("SourceCatalog: Unable to find source catalog entry with logical source name {}", logicalSourceName);
        throw Exceptions::PhysicalSourceNotFoundException("SourceCatalog: Logical source(s) [" + logicalSourceName
                                                          + "] are found to have no physical source(s) defined. ");
    }
    return logicalToPhysicalSourceMapping[logicalSourceName];
}

std::map<std::string, SchemaPtr> SourceCatalog::getAllLogicalSource() { return logicalSourceNameToSchemaMapping; }

std::map<std::string, std::string> SourceCatalog::getAllLogicalSourceAsString() {
    std::map<std::string, std::string> allLogicalSourceAsString;
    const std::map<std::string, SchemaPtr> allLogicalSource = getAllLogicalSource();

    for (auto const& [name, schema] : allLogicalSource) {
        allLogicalSourceAsString[name] = schema->toString();
    }
    return allLogicalSourceAsString;
}

nlohmann::json SourceCatalog::getAllLogicalSourcesAsJson() {
    nlohmann::json logicalSources;
    const auto& allLogicalSourceAsString = getAllLogicalSourceAsString();
    for (auto const& [key, val] : allLogicalSourceAsString) {
        nlohmann::json entry;
        entry[key] = val;
        logicalSources.push_back(entry);
    }
    return logicalSources;
}

nlohmann::json SourceCatalog::getPhysicalSourcesAsJson(std::string logicalSourceName) {
    const std::vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSource = getPhysicalSources(logicalSourceName);
    nlohmann::json result;
    nlohmann::json::array_t allSource = {};
    for (auto const& physicalSource : std::as_const(allPhysicalSource)) {
        allSource.push_back(physicalSource->toJson());
    }
    result["physicalSources"] = allSource;
    return result;
}

nlohmann::json SourceCatalog::getLogicalSourceAsJson(std::string logicalSourceName) {
    auto schema = getLogicalSourceOrThrowException(logicalSourceName)->getSchema();
    auto serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    nlohmann::json json;
    json["schema"] = serializableSchema->SerializeAsString();
    return json;
}

bool SourceCatalog::updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schema) {
    //check if source already exist
    NES_DEBUG("SourceCatalog: search for logical source in addLogicalSource() {}", logicalSourceName);

    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR("SourceCatalog: Unable to find logical source {} to update.", logicalSourceName);
        return false;
    }
    NES_TRACE("SourceCatalog: create a new schema object and add to the catalog");
    logicalSourceNameToSchemaMapping[logicalSourceName] = std::move(schema);
    return true;
}

std::pair<bool, std::string> SourceCatalog::addPhysicalSource(const std::string& physicalSourceName,
                                                              const std::string& logicalSourceName,
                                                              const std::string& physicalSourceTypeName,
                                                              WorkerId topologyNodeId) {

    if (!containsLogicalSource(logicalSourceName)) {
        NES_ERROR("SourceCatalogService::RegisterPhysicalSource logical source does not exist {}", logicalSourceName);
        return {false, fmt::format("RegisterPhysicalSource logical source does not exist {}", logicalSourceName)};
    }

    NES_DEBUG("SourceCatalogService::RegisterPhysicalSource: try to register physical node id {} physical source= {} logical "
              "source= {}",
              topologyNodeId,
              physicalSourceName,
              logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, physicalSourceName, physicalSourceTypeName);
    auto logicalSource = getLogicalSource(logicalSourceName);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, topologyNodeId);
    bool success = addPhysicalSource(logicalSourceName, sce);
    if (!success) {
        NES_ERROR("SourceCatalogService::RegisterPhysicalSource: adding physical source was not successful.");
        return {false, "RegisterPhysicalSource: adding physical source was not successful."};
    }
    return {success, ""};
}

std::pair<bool, std::string> SourceCatalog::addPhysicalSource(const std::string& physicalSourceName,
                                                              const std::string& logicalSourceName,
                                                              WorkerId topologyNodeId) {
    return SourceCatalog::addPhysicalSource(physicalSourceName, logicalSourceName, "", topologyNodeId);
}

void SourceCatalog::setKeyDistributionMap(std::map<SourceCatalogEntryPtr, std::set<uint64_t>>& distributionMap) {
    this->keyDistributionMap = distributionMap;
}

std::map<SourceCatalogEntryPtr, std::set<uint64_t>>& SourceCatalog::getKeyDistributionMap() { return keyDistributionMap; }

}// namespace NES::Catalogs::Source
