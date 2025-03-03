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

#include <Util/QuerySignatures/QuerySignature.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignature::create(z3::ExprPtr&& conditions,
                                         std::vector<std::string>&& columns,
                                         std::vector<std::map<std::string, z3::ExprPtr>>&& schemaFieldToExprMaps,
                                         std::vector<std::map<std::string, z3::ExprPtr>>&& windowsExpressions,
                                         std::map<std::string, z3::ExprPtr>&& unionExpressions) {
    return std::make_shared<QuerySignature>(QuerySignature(std::move(conditions),
                                                           std::move(columns),
                                                           std::move(schemaFieldToExprMaps),
                                                           std::move(windowsExpressions),
                                                           std::move(unionExpressions)));
}

QuerySignature::QuerySignature(z3::ExprPtr&& conditions,
                               std::vector<std::string>&& columns,
                               std::vector<std::map<std::string, z3::ExprPtr>>&& schemaFieldToExprMaps,
                               std::vector<std::map<std::string, z3::ExprPtr>>&& windowsExpressions,
                               std::map<std::string, z3::ExprPtr>&& unionExpressions)
    : conditions(std::move(conditions)), columns(std::move(columns)), schemaFieldToExprMaps(std::move(schemaFieldToExprMaps)),
      windowsExpressions(std::move(windowsExpressions)), unionExpressions(std::move(unionExpressions)) {}

z3::ExprPtr QuerySignature::getConditions() { return conditions; }

const std::vector<std::string>& QuerySignature::getColumns() { return columns; }

const std::vector<std::map<std::string, z3::ExprPtr>>& QuerySignature::getWindowsExpressions() { return windowsExpressions; }

const std::vector<std::map<std::string, z3::ExprPtr>>& QuerySignature::getSchemaFieldToExprMaps() {
    return schemaFieldToExprMaps;
}

const std::map<std::string, z3::ExprPtr>& QuerySignature::getUnionExpressions() { return unionExpressions; }

}// namespace NES::Optimizer
