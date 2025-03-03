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

#ifndef NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_
#define NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <list>
#include <map>
#include <memory>
#include <set>

namespace NES {

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

using PlacedDecomposedQueryPlans = std::map<SharedQueryId, std::map<DecomposedQueryId, DecomposedQueryPlanPtr>>;

/**
 * This class contains information about the physical node, a map of decomposed query plans that need to be executed
 * on the physical node, and some additional configurations.
 */
class ExecutionNode : public Node {

  public:
    static ExecutionNodePtr create(WorkerId WorkerId);

    explicit ExecutionNode(WorkerId WorkerId);

    virtual ~ExecutionNode() = default;

    /**
     * Get execution node id
     * @return id of the execution node
     */
    WorkerId getId() const;

    /**
     * Register a new decomposed query plan
     * @param decomposedQueryPlan : the decomposed query plan
     * @return true if operation is successful
     */
    bool registerDecomposedQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * Update decomposed query plans for the given shared query plan id
     * @note: We store a copy of the supplied decomposed query plans
     * @param sharedQueryId : the shared query id
     * @param decomposedQueryPlans : the decomposed query plans
     */
    void updateDecomposedQueryPlans(SharedQueryId sharedQueryId, std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans);

    /**
     * @brief Get decomposed query plans belonging to the given shared query Id
     * @param sharedQueryId: the shared query id
     * @return vector containing placed decomposed query plans
     */
    std::vector<DecomposedQueryPlanPtr> getAllDecomposedQueryPlans(SharedQueryId sharedQueryId) const;

    /**
      * @brief Get the decomposed query plan belonging to a given shared query id and has the provided decomposed query plan id
      * @param sharedQueryId: shared query id
      * @param decomposedQueryId: decomposed query plan id
      * @return the decomposed query plan
      */
    DecomposedQueryPlanPtr getDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) const;

    /**
     * Remove existing decomposed query plans belonging to a shared query plan
     * @param sharedQueryId: the shared query plans
     * @return true if operation succeeds
     */
    bool removeDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * Check if there are registered decomposed query plans hosted on the execution node
     * @param sharedQueryId: the shared query plans
     * @return true if operation succeeds
     */
    bool hasRegisteredDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * Remove a decomposed query plan belonging to a shared query plan with given decomposed query plan id
     * @param sharedQueryId the id of the shared query
     * @param decomposedQueryId the id of the decomposed query plan to remove
     * @return true if operation succeeds
     */
    bool removeDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * Get the map of all decomposed query plans
     * @return map of shared query plan id to decomposed query plans
     */
    PlacedDecomposedQueryPlans getAllQuerySubPlans();

    /**
     * @brief Get identifier of all shared query plans placed on the execution node
     * @return set of shared query plan ids
     */
    std::set<SharedQueryId> getPlacedSharedQueryPlanIds() const;

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;

    std::vector<std::string> toMultilineString() override;

  private:
    /**
     * Execution node id.
     * Same as physical node id.
     */
    const WorkerId workerId;

    /**
     * a map of placed decomposed query plans
     */
    PlacedDecomposedQueryPlans mapOfSharedQueryToDecomposedQueryPlans;
};
}// namespace Optimizer
}// namespace NES

#endif// NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_
