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

#ifndef NES_OPERATORS_INCLUDE_UTIL_QUERYSIGNATURECONTEXT_HPP_
#define NES_OPERATORS_INCLUDE_UTIL_QUERYSIGNATURECONTEXT_HPP_
#include <memory>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

class QuerySignatureContext {
  public:
    virtual QuerySignaturePtr createQuerySignatureForOperator(const OperatorPtr& operatorNode) const = 0;
    virtual ~QuerySignatureContext() = default;
};
}// namespace NES::Optimizer

#endif// NES_OPERATORS_INCLUDE_UTIL_QUERYSIGNATURECONTEXT_HPP_
