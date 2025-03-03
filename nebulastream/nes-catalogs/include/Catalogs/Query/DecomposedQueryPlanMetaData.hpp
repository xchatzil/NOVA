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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_DECOMPOSEDQUERYPLANMETADATA_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_DECOMPOSEDQUERYPLANMETADATA_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <Util/QueryStateHistory.hpp>
#include <memory>

namespace NES {

class DecomposedQueryPlanMetaData;
using DecomposedQueryPlanMetaDataPtr = std::shared_ptr<DecomposedQueryPlanMetaData>;

/**
 * This class stores metadata about a decomposed query plan.
 */
class DecomposedQueryPlanMetaData {

  public:
    static DecomposedQueryPlanMetaDataPtr create(DecomposedQueryId decomposedQueryId,
                                                 DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                                 QueryState decomposedQueryPlanState,
                                                 WorkerId workerId);

    /**
     * Update the status of the qub query
     * @param newDecomposedQueryPlanState : new state
     */
    void updateState(QueryState newDecomposedQueryPlanState);

    /**
     * Update the termination reason
     * @param terminationReason : information to update
     */
    void setTerminationReason(const std::string& terminationReason);

    /**
     * Get status of query sub plan
     * @return status
     */
    QueryState getDecomposedQueryPlanStatus() const;

    /**
     * @brief Get the decomposed query id
     * @return id of the decomposed query
     */
    DecomposedQueryId getDecomposedQueryId() const;

    /**
     * @brief Get the worker id where decomposed plan is located
     * @return id of the worker
     */
    WorkerId getWorkerId() const;

    /**
     * @brief String rep of the meta information
     * @return meta data as string``
     */
    const std::string& getTerminationReason() const;

    /** @brief Retrieve a timestamped history of query status changes. */
    const QueryStateHistory& getHistory() const;

    /**
     * @brief Get decomposed query plan version
     * @return decomposed query plan version
     */
    DecomposedQueryPlanVersion getDecomposedQueryPlanVersion() const;

    DecomposedQueryPlanMetaData(DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                QueryState decomposedQueryPlanState,
                                WorkerId workerId);

  private:
    /**
     * Id of the decomposed query plan
     */
    DecomposedQueryId decomposedQueryId;

    /**
     * @brief the current version of the decomposed query plan
     */
    DecomposedQueryPlanVersion decomposedQueryPlanVersion;

    /**
     * status of the sub query
     */
    QueryState decomposedQueryPlanState;

    /**
     * @brief Stores a history of QueryState updates with their timestamp in microseconds.
     */
    QueryStateHistory history;

    /**
     * worker id where the sub query is deployed
     */
    WorkerId workerId;

    /**
     * Any addition meta information e.g., failure reason
     */
    std::string terminationReason;
};
}// namespace NES

#endif// NES_CATALOGS_INCLUDE_CATALOGS_QUERY_DECOMPOSEDQUERYPLANMETADATA_HPP_
