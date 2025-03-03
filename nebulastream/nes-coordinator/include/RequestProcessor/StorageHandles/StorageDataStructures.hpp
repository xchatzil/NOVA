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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEDATASTRUCTURES_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEDATASTRUCTURES_HPP_

#include <folly/concurrency/UnboundedQueue.h>
#include <memory>

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Catalogs::UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace Catalogs::UDF

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace Statistic {
class StatisticProbeHandler;
using StatisticProbeHandlerPtr = std::shared_ptr<StatisticProbeHandler>;
}// namespace Statistic

namespace RequestProcessor {
/**
 * @brief This struct contains smart pointers to the data structures which coordinator requests operate on.
 */
struct StorageDataStructures {
    StorageDataStructures(Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                          TopologyPtr topology,
                          Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                          GlobalQueryPlanPtr globalQueryPlan,
                          Catalogs::Query::QueryCatalogPtr queryCatalog,
                          Catalogs::Source::SourceCatalogPtr sourceCatalog,
                          Catalogs::UDF::UDFCatalogPtr udfCatalog,
                          Statistic::StatisticProbeHandlerPtr statisticProbeHandler);

    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
};
}// namespace RequestProcessor
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_STORAGEHANDLES_STORAGEDATASTRUCTURES_HPP_
