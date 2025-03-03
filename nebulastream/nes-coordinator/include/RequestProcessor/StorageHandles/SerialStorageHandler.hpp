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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_SERIALSTORAGEHANDLER_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_SERIALSTORAGEHANDLER_HPP_

#include <RequestProcessor/StorageHandles/StorageHandler.hpp>

namespace NES::RequestProcessor {
struct StorageDataStructures;
/**
 * @brief This class is intended for serial access and does not perform any locking before creating a resource handle.
 * Not thread safe!
 */
class SerialStorageHandler : public StorageHandler {
  public:
    /**
     * @brief factory to create a serial storage manager object
     * @param storageDataStructures a struct containing pointers to the following data structures:
     * -globalExecutionPlan
     * -topology
     * -queryCatalogService
     * -globalQueryPlan
     * -sourceCatalog
     * -udfCatalog
     * -lockManager
     * -statisticProbeHandler
     * @return shared pointer to the serial storage manager
     */
    static StorageHandlerPtr create(StorageDataStructures storageDataStructures);

    /**
     * @brief Obtain a mutable topology handle.
     * @param requestId The id of the request making the call
     * @return a handle to the topology
     */
    TopologyHandle getTopologyHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable query catalog handle.
     * @param requestId The id of the request making the call
     * @return a handle to the query catalog.
     */
    QueryCatalogHandle getQueryCatalogHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable source catalog handle.
     * @param requestId The id of the request making the call
     * @return a handle to the source catalog.
     */
    Catalogs::Source::SourceCatalogPtr getSourceCatalogHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable global execution plan handle.
     * @param requestId The id of the request making the call
     * @return a handle to the global execution plan.
     */
    Optimizer::GlobalExecutionPlanPtr getGlobalExecutionPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable global query plan handle.
     * @param requestId The id of the request making the call
     * @return a handle to the global query plan.
     */
    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable udf catalog handle.
     * @param requestId The id of the request making the call
     * @return a handle to the udf catalog.
     */
    Catalogs::UDF::UDFCatalogPtr getUDFCatalogHandle(RequestId requestId) override;

    /**
     * @brief Obtain a mutable Coordinator configuration
     * @param requestId The id of the request making the call
     * @return a handle to the coordinator configuration
     */
    Configurations::CoordinatorConfigurationPtr getCoordinatorConfiguration(RequestId requestId) override;

    /**
     * @brief Obtain a mutable Statistic probe handler
     * @param requestId The id of the request making the call
     * @return a handle to the statistic probe handler
     */
    Statistic::StatisticProbeHandlerPtr getStatisticProbeHandler(RequestId requestId) override;

    /**
     * @brief constructor
     * @param storageDataStructures a struct containing pointers to the following data structures:
     * -globalExecutionPlan
     * -topology
     * -queryCatalogService
     * -globalQueryPlan
     * -sourceCatalog
     * -udfCatalog
     * -lockManager
     */
    explicit SerialStorageHandler(StorageDataStructures& storageDataStructures);

  private:
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
};
}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_SERIALSTORAGEHANDLER_HPP_
