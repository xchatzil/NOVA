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

#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/Selectivity.hpp>

namespace NES::Statistic {
MetricPtr Selectivity::create(const FieldAccessExpressionNodePtr& expressionNode) {
    return std::make_shared<Selectivity>(Selectivity(expressionNode));
}

bool Selectivity::operator==(const StatisticMetric& rhs) const {
    if (rhs.instanceOf<Selectivity>()) {
        // We assume that if the field has the same name, the metric is equal
        auto rhsSelectivity = dynamic_cast<const Selectivity&>(rhs);
        return field->getFieldName() == rhsSelectivity.field->getFieldName();
    }
    return false;
}

std::string Selectivity::toString() const { return "Selectivity over " + field->toString(); }

Selectivity::Selectivity(const FieldAccessExpressionNodePtr& expressionNode) : StatisticMetric(expressionNode) {}
}// namespace NES::Statistic
