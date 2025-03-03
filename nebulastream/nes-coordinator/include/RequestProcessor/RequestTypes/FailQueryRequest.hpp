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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <memory>

namespace NES {

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class PlacementAmendmentHandler;
using PlacementAmendmentHandlerPtr = std::shared_ptr<PlacementAmendmentHandler>;
}// namespace Optimizer

namespace RequestProcessor {
class FailQueryRequest;
using FailQueryRequestPtr = std::shared_ptr<FailQueryRequest>;

//a response to the creator of the request
struct FailQueryResponse : public AbstractRequestResponse {
    explicit FailQueryResponse(SharedQueryId sharedQueryId) : sharedQueryId(sharedQueryId){};
    SharedQueryId sharedQueryId;
};

class FailQueryRequest : public AbstractUniRequest {
  public:
    /**
     * @brief Constructor
     * @param sharedQueryId: The id of the shared query that failed
     * @param failedDecomposedPlanId: The id of the decomposed plan that caused the failure
     * @param failureReason: the failure reason
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param placementAmendmentHandler: placement amendment handler for updating and un-deploying the operators of the failed shared query
     */
    FailQueryRequest(SharedQueryId sharedQueryId,
                     DecomposedQueryId failedDecomposedPlanId,
                     const std::string& failureReason,
                     uint8_t maxRetries,
                     const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler);

    /**
    * @brief creates a new FailQueryRequest object
    * @param sharedQueryId: The id of the shared query that failed
    * @param failedDecomposedQueryId: The id of the decomposed plan that caused the failure
    * @param failureReason: the failure reason
    * @param maxRetries: Maximum number of retry attempts for the request
    * @return a smart pointer to the newly created object
    */
    static FailQueryRequestPtr create(SharedQueryId sharedQueryId,
                                      DecomposedQueryId failedDecomposedQueryId,
                                      const std::string& failureReason,
                                      uint8_t maxRetries,
                                      const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    void postExecution(const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;

  private:
    SharedQueryId sharedQueryId;
    DecomposedQueryId decomposedQueryId;
    std::string failureReason;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    Optimizer::PlacementAmendmentHandlerPtr placementAmendmentHandler;
};
}// namespace RequestProcessor
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_FAILQUERYREQUEST_HPP_
