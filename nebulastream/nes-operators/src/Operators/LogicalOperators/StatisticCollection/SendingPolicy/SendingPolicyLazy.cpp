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

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>

namespace NES::Statistic {

std::string SendingPolicyLazy::toString() const { return "LAZY"; }

SendingPolicyPtr SendingPolicyLazy::create(StatisticDataCodec sinkDataCodec) {
    return std::make_shared<SendingPolicyLazy>(SendingPolicyLazy(sinkDataCodec));
}

bool SendingPolicyLazy::operator==(const SendingPolicy& rhs) const { return rhs.instanceOf<const SendingPolicyLazy>(); }

SendingPolicyLazy::SendingPolicyLazy(StatisticDataCodec sinkDataCodec) : SendingPolicy(sinkDataCodec) {}

}// namespace NES::Statistic
