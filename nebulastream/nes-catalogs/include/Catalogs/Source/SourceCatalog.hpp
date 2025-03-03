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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_

#include <API/Schema.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <deque>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <vector>

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Catalogs::Source {

/**
 * @brief the source catalog handles the mapping of logical to physical sources
 */
class SourceCatalog {
  public:
    /**
   * @brief method to add a logical source
   * @param logicalSourceName logical source name
   * @param schema of logical source as object
   * @return bool indicating if insert was successful
   */
    bool addLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

    /**
       * @brief method to delete a logical source
       * @caution this method only remove the entry from the catalog not from the topology
       * @param logicalSourceName name of logical source to delete
       * @return bool indicating the success of the removal
       */
    bool removeLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief method to add a physical source
     * @caution combination of node and name has to be unique
     * @param logicalSourceName logical source name
     * @param sourceCatalogEntry the source catalog entry to store
     * @return bool indicating success of insert source
     */
    bool addPhysicalSource(const std::string& logicalSourceName, const SourceCatalogEntryPtr& sourceCatalogEntry);

    /**
     * @brief method to remove a physical source
     * @param logicalSourceName name of the logical source
     * @param physicalSourceName name of the physical source
     * @param topologyNodeId id of the topology node
     * @return bool indicating success of remove source
     */
    bool
    removePhysicalSource(const std::string& logicalSourceName, const std::string& physicalSourceName, WorkerId topologyNodeId);

    /**
     * @brief method to remove all physical sources of a single worker
     * @param workerId worker node identifier
     * @return number of sucessfully removed physical sources
     */
    size_t removeAllPhysicalSourcesByWorker(WorkerId workerId);

    /**
     * @brief method to get the schema from the given logical source
     * @param logicalSourceName name of the logical source name
     * @return the pointer to the schema
     */
    SchemaPtr getSchemaForLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief method to return the logical source for an existing logical source
     * @param logicalSourceName name of the logical source
     * @return smart pointer to the logical source else nullptr
     * @note the source will also contain the schema
     */
    LogicalSourcePtr getLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief method to return the source for an existing logical source or throw exception
     * @param logicalSourceName name of logical source
     * @return smart pointer to a physical source else nullptr
     * @note the source will also contain the schema
     * @throws RuntimeException
     */
    LogicalSourcePtr getLogicalSourceOrThrowException(const std::string& logicalSourceName);

    /**
     * @brief Check if logical source with this name exists
     * @param logicalSourceName name of the logical source
     * @return bool indicating if source exists
     */
    bool containsLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief return all topology node ids that host physical sources that contribute to this logical source
     * @param logicalSourceName name of logical source
     * @return list of topology nodes ids
     */
    std::vector<WorkerId> getSourceNodesForLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief reset the catalog and recreate the default_logical source
     * @return bool indicating success
     */
    bool reset();

    /**
     * @brief Return a list of logical source names registered at catalog
     * @return map containing source name as key and schema object as value
     */
    std::map<std::string, SchemaPtr> getAllLogicalSource();

    /**
     * @brief Get all logical sources with their schema as string
     * @return map of logical source name to schema as string
     */
    std::map<std::string, std::string> getAllLogicalSourceAsString();

    /**
     * @brief Get a json containing all logical sources with their schema as string
     * @return json containing logical source name to schema as string
     */
    nlohmann::json getAllLogicalSourcesAsJson();

    /**
     * @brief Get a json containing all physical sources belonging to a physical source
     * @return json containing the physical sources
     */
    nlohmann::json getPhysicalSourcesAsJson(std::string logicalSourceName);

    /**
     * @brief Get a json containing the schema of a logical source
     * @return json containing the schema of the source
     */
    nlohmann::json getLogicalSourceAsJson(std::string logicalSourceName);

    /**
     * @brief method to return the physical source and the associated schemas
     * @return string containing the content of the catalog
     */
    std::string getPhysicalSourceAndSchemaAsString();

    /**
     * @brief get all physical sources for a logical source
     * @param logicalSourceName name of the logical source
     * @return vector containing source catalog entries
     */
    std::vector<SourceCatalogEntryPtr> getPhysicalSources(const std::string& logicalSourceName);

    /**
     * @brief method to update a logical source schema
     * @param logicalSourceName logical source name
     * @param schema of logical source as object
     * @return bool indicating if update was successful
     */
    bool updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

    /**
     * @brief method to register a physical source
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     * @param physicalSourceTypeName: string representation of the physical source type
     * @param topologyNodeId : the topology node id
     * @return bool indicating success
     */
    std::pair<bool, std::string> addPhysicalSource(const std::string& physicalSourceName,
                                                   const std::string& logicalSourceName,
                                                   const std::string& physicalSourceTypeName,
                                                   WorkerId topologyNodeId);

    /**
     * @brief method to register a physical source
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     *
     * @param topologyNodeId : the topology node id
     * @return bool indicating success
     */
    std::pair<bool, std::string>
    addPhysicalSource(const std::string& physicalSourceName, const std::string& logicalSourceName, WorkerId topologyNodeId);

    /**
     * Gets the key distribution for a given source catalog entry.
     * @param catalogEntry
     * @return the key distribution
     */
    std::map<SourceCatalogEntryPtr, std::set<uint64_t>>& getKeyDistributionMap();

    /**
     * Set the key distribution map for all source catalog entries
     * @param distributionMap
     */
    void setKeyDistributionMap(std::map<SourceCatalogEntryPtr, std::set<uint64_t>>& distributionMap);

    SourceCatalog();

  private:
    /**
     * @brief test if logical source with this name exists
     * @param logicalSourceName name of the logical source to test
     * @return bool indicating if source exists
     */
    bool testIfLogicalSourceExistsInLogicalToPhysicalMapping(const std::string& logicalSourceName);

    //map logical source to schema
    std::map<std::string, SchemaPtr> logicalSourceNameToSchemaMapping;
    //map logical source to physical source
    std::map<std::string, std::vector<SourceCatalogEntryPtr>> logicalToPhysicalSourceMapping;
    //map with value distribution for physical sources
    //FIXME if #4606 is solved, this will be removed
    std::map<SourceCatalogEntryPtr, std::set<uint64_t>> keyDistributionMap;

    void addDefaultSources();
};
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_
