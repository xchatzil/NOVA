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
#include <Operators/LogicalOperators/StatisticCollection/Metrics/MinVal.hpp>

namespace NES::Statistic {

MetricPtr MinVal::create(const FieldAccessExpressionNodePtr& field) { return std::make_shared<MinVal>(MinVal(field)); }

MinVal::MinVal(const FieldAccessExpressionNodePtr& field) : StatisticMetric(field) {}

bool MinVal::operator==(const StatisticMetric& rhs) const {
    if (rhs.instanceOf<MinVal>()) {
        // We assume that if the field has the same name, the metric is equal
        auto rhsMinVal = dynamic_cast<const MinVal&>(rhs);
        return field->getFieldName() == rhsMinVal.field->getFieldName();
    }
    return false;
}

std::string MinVal::toString() const { return "MinVal over " + field->toString(); }
}// namespace NES::Statistic
