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

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>

namespace NES::Statistic {

std::string SendingPolicyASAP::toString() const { return "ASAP"; }

SendingPolicyPtr SendingPolicyASAP::create(StatisticDataCodec sinkDataCodec) {
    return std::make_shared<SendingPolicyASAP>(SendingPolicyASAP(sinkDataCodec));
}

bool SendingPolicyASAP::operator==(const SendingPolicy& rhs) const { return rhs.instanceOf<const SendingPolicyASAP>(); }

SendingPolicyASAP::SendingPolicyASAP(StatisticDataCodec sinkDataCodec) : SendingPolicy(sinkDataCodec) {}

}// namespace NES::Statistic
