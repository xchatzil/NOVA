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

#ifndef NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3EXPRANDFIELDMAP_HPP_
#define NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3EXPRANDFIELDMAP_HPP_

#include <map>
#include <memory>
#include <string>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;
}// namespace z3

namespace NES::Optimizer {
class Z3ExprAndFieldMap;
using Z3ExprAndFieldMapPtr = std::shared_ptr<Z3ExprAndFieldMap>;

/**
 * @brief This class is responsible for holding the Z3 expression and a map of attribute name to corresponding z3 expression
 *
 * Example:
 *      for an expression Attribute("x") = 40
 *
 *      This class holds following value expr => z3StrVar("x") == z3IntConst(40) and constMap => {{"x", z3StrVar("x")}}
 *
 * This class is used in replacing the field expressions later on.
 *
 */
class Z3ExprAndFieldMap {

  public:
    static Z3ExprAndFieldMapPtr create(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap);

    /**
     * @brief Get the Z3 expression pointer
     * @return the pointer to the z3 expression
     */
    z3::ExprPtr getExpr();

    /**
     * @brief Get the filed map containing the map of filed name ot z3 expressions
     * @return the field map
     */
    std::map<std::string, z3::ExprPtr> getFieldMap();

  private:
    Z3ExprAndFieldMap(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> fieldMap);

    z3::ExprPtr expr;
    std::map<std::string, z3::ExprPtr> fieldMap;
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3EXPRANDFIELDMAP_HPP_
