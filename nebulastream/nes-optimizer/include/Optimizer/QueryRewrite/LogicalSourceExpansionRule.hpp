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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_LOGICALSOURCEEXPANSIONRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_LOGICALSOURCEEXPANSIONRULE_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <memory>
#include <set>
#include <unordered_map>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

}// namespace NES

namespace NES::Optimizer {
class LogicalSourceExpansionRule;
using LogicalSourceExpansionRulePtr = std::shared_ptr<LogicalSourceExpansionRule>;

const std::string LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS = "ListOfBlockingDownStreamOperatorIds";
const std::string LIST_OF_SIBLING_STATISTIC_IDS = "ListOfSiblingStatisticIds";

/**
 * @brief This class will expand the logical query graph by adding information about the physical sources and expand the
 * graph by adding additional replicated operators.
 *
 * Note: Apart from replicating the logical source operator we also replicate its down stream operators till we
 * encounter first blocking operator as defined by method @see LogicalSourceExpansionRule:isBlockingOperator.
 *
 * Note: if expandSourceOnly is set to true then only source operators will be expanded and @see LogicalSourceExpansionRule:isBlockingOperator
 * will be ignored.
 *
 * Example: a query :                       Sink
 *                                           |
 *                                           Map
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                            Sink
 *                                                /     \
 *                                              /        \
 *                                           Map1        Map2
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 *
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2
 *
 *                                                     or
 *
 * a query :                                    Sink
 *                                               |
 *                                             Merge
 *                                             /   \
 *                                            /    Filter
 *                                          /        |
 *                                   Source(Car)   Source(Truck)
 *
 * will be expanded to:                             Sink
 *                                                   |
 *                                                 Merge
 *                                              /  / \   \
 *                                            /  /    \    \
 *                                         /   /       \     \
 *                                       /   /          \      \
 *                                    /    /         Filter1   Filter2
 *                                 /     /               \         \
 *                               /      /                 \          \
 *                         Src(Car1) Src(Car2)       Src(Truck1)  Src(Truck2)
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2 and
 * logical source truck has two physical sources: i.e. truck1 and truck2
 *
 */
class LogicalSourceExpansionRule : public BaseRewriteRule {
  public:
    static LogicalSourceExpansionRulePtr create(const Catalogs::Source::SourceCatalogPtr&, bool expandSourceOnly);

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    virtual ~LogicalSourceExpansionRule() = default;

  private:
    explicit LogicalSourceExpansionRule(const Catalogs::Source::SourceCatalogPtr&, bool expandSourceOnly);

    /**
     * @brief This method starts from an operator and traverse upstream if the corresponding upstream(parent) operator is not a
     * blocking operator (@see isBlockingOperator). If the upstream operator is a blocking operator then it is removed as the operator's upstream
     * operator and its information is stored in the operators property.
     * @param operatorNode : operator to check for connected blocking operator
     */
    void removeConnectedBlockingOperators(const NodePtr& operatorNode);

    /**
     * @brief Add the upstream operator id to the operator property
     * @param operatorNode operator whose property needs to be updated
     * @param downStreamOperatorId id of the downstream operator to add
     */
    void addBlockingDownStreamOperator(const NodePtr& operatorNode, OperatorId downStreamOperatorId);

    /**
     * @brief Check if the input operator is a blocking operator or not (operator that can't be expanded, for example, Window Join or Union)
     * @param operatorNode : operator to check
     * @return true if blocking else false
     */
    bool isBlockingOperator(const NodePtr& operatorNode);

    /**
     * @brief Distributes the new statistic ids to all siblings. After this method, all sibling operators have different
     * statistic ids.
     * @param queryPlan
     * @param siblingStatisticIdToNewStatisticIds: Stores for each sibling id their new statistic ids.
     */
    void distributeSiblingStatisticId(
        QueryPlan& queryPlan,
        std::unordered_map<StatisticId, std::vector<StatisticId>>& siblingStatisticIdToNewStatisticIds) const;

    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    bool expandSourceOnly;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_LOGICALSOURCEEXPANSIONRULE_HPP_
