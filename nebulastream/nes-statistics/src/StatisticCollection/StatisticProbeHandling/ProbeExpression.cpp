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

#include <StatisticCollection/StatisticProbeHandling/ProbeExpression.hpp>

namespace NES::Statistic {

ProbeExpression::ProbeExpression(const ExpressionNodePtr& probeExpression) : probeExpression(probeExpression) {}

const ExpressionNodePtr& ProbeExpression::getProbeExpression() const { return probeExpression; }
}// namespace NES::Statistic
