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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_NEGATEOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_NEGATEOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {

class NegateOperation : public Operation {
  public:
    NegateOperation(OperationIdentifier identifier, OperationPtr input);
    ~NegateOperation() override = default;
    OperationPtr getInput();
    std::string toString() override;
    bool classof(const Operation* Op);

  private:
    OperationWPtr input;
};
}// namespace NES::Nautilus::IR::Operations
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_NEGATEOPERATION_HPP_
