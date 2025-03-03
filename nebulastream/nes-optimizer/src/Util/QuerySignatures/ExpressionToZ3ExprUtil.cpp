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

#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatures/DataTypeToZ3ExprUtil.hpp>
#include <Util/QuerySignatures/ExpressionToZ3ExprUtil.hpp>
#include <Util/QuerySignatures/Z3ExprAndFieldMap.hpp>
#include <z3++.h>

namespace NES::Optimizer {

Z3ExprAndFieldMapPtr ExpressionToZ3ExprUtil::createForExpression(const ExpressionNodePtr& expression,
                                                                 const z3::ContextPtr& context) {
    NES_DEBUG("Creating Z3 expression for input expression {}", expression->toString());
    if (expression->instanceOf<LogicalExpressionNode>()) {
        return createForLogicalExpressions(expression, context);
    }
    if (expression->instanceOf<ArithmeticalExpressionNode>()) {
        return createForArithmeticalExpressions(expression, context);
    } else if (expression->instanceOf<ConstantValueExpressionNode>()) {
        auto constantValueExpression = expression->as<ConstantValueExpressionNode>();
        auto value = constantValueExpression->getConstantValue();
        return DataTypeToZ3ExprUtil::createForDataValue(value, context);
    } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
        auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
        std::string fieldName = fieldAccessExpression->getFieldName();
        DataTypePtr fieldType = fieldAccessExpression->getStamp();
        return DataTypeToZ3ExprUtil::createForField(fieldName, fieldType, context);
    } else if (expression->instanceOf<FieldAssignmentExpressionNode>()) {
        auto fieldAssignmentExpressionNode = expression->as<FieldAssignmentExpressionNode>();
        return createForExpression(fieldAssignmentExpressionNode->getAssignment(), context);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the expression: " + expression->toString());
    return nullptr;
}

Z3ExprAndFieldMapPtr ExpressionToZ3ExprUtil::createForArithmeticalExpressions(const ExpressionNodePtr& expression,
                                                                              const z3::ContextPtr& context) {
    NES_DEBUG("Create Z3 expression for arithmetical expression {}", expression->toString());
    if (expression->instanceOf<AddExpressionNode>()) {
        auto addExpressionNode = expression->as<AddExpressionNode>();
        auto left = createForExpression(addExpressionNode->getLeft(), context);
        auto right = createForExpression(addExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_add(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    }
    if (expression->instanceOf<SubExpressionNode>()) {
        auto subExpressionNode = expression->as<SubExpressionNode>();
        auto left = createForExpression(subExpressionNode->getLeft(), context);
        auto right = createForExpression(subExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_sub(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<MulExpressionNode>()) {
        auto mulExpressionNode = expression->as<MulExpressionNode>();
        auto left = createForExpression(mulExpressionNode->getLeft(), context);
        auto right = createForExpression(mulExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_mul(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<DivExpressionNode>()) {
        auto divExpressionNode = expression->as<DivExpressionNode>();
        auto left = createForExpression(divExpressionNode->getLeft(), context);
        auto right = createForExpression(divExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_div(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression implemented for the arithmetical expression node: "
                            + expression->toString());
    return nullptr;
}

Z3ExprAndFieldMapPtr ExpressionToZ3ExprUtil::createForLogicalExpressions(const ExpressionNodePtr& expression,
                                                                         const z3::ContextPtr& context) {
    NES_DEBUG("Create Z3 expression node for logical expression {}", expression->toString());
    if (expression->instanceOf<AndExpressionNode>()) {
        auto andExpressionNode = expression->as<AndExpressionNode>();
        auto left = createForExpression(andExpressionNode->getLeft(), context);
        auto right = createForExpression(andExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    }
    if (expression->instanceOf<OrExpressionNode>()) {
        auto orExpressionNode = expression->as<OrExpressionNode>();
        auto left = createForExpression(orExpressionNode->getLeft(), context);
        auto right = createForExpression(orExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        Z3_ast array[] = {*left->getExpr(), *right->getExpr()};
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_or(*context, 2, array)));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<LessExpressionNode>()) {
        auto lessExpressionNode = expression->as<LessExpressionNode>();
        auto left = createForExpression(lessExpressionNode->getLeft(), context);
        auto right = createForExpression(lessExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());

        if (left->getExpr()->is_fpa() || right->getExpr()->is_fpa()) {
            auto expr =
                std::make_shared<z3::expr>(to_expr(*context, Z3_mk_fpa_lt(*context, *left->getExpr(), *right->getExpr())));
            return Z3ExprAndFieldMap::create(expr, leftFieldMap);
        }
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_lt(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        auto lessEqualsExpressionNode = expression->as<LessEqualsExpressionNode>();
        auto left = createForExpression(lessEqualsExpressionNode->getLeft(), context);
        auto right = createForExpression(lessEqualsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());
        if (left->getExpr()->is_fpa() || right->getExpr()->is_fpa()) {
            auto expr =
                std::make_shared<z3::expr>(to_expr(*context, Z3_mk_fpa_leq(*context, *left->getExpr(), *right->getExpr())));
            return Z3ExprAndFieldMap::create(expr, leftFieldMap);
        }
        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_le(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        auto greaterExpressionNode = expression->as<GreaterExpressionNode>();
        auto left = createForExpression(greaterExpressionNode->getLeft(), context);
        auto right = createForExpression(greaterExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());
        std::shared_ptr<z3::expr> expr;
        if (left->getExpr()->is_fpa() || right->getExpr()->is_fpa()) {
            expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_fpa_gt(*context, *left->getExpr(), *right->getExpr())));
        } else {
            expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_gt(*context, *left->getExpr(), *right->getExpr())));
        }

        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        auto greaterEqualsExpressionNode = expression->as<GreaterEqualsExpressionNode>();
        auto left = createForExpression(greaterEqualsExpressionNode->getLeft(), context);
        auto right = createForExpression(greaterEqualsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFieldMap = left->getFieldMap();
        leftFieldMap.merge(right->getFieldMap());
        std::shared_ptr<z3::expr> expr;
        if (left->getExpr()->is_fpa() || right->getExpr()->is_fpa()) {
            expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_fpa_geq(*context, *left->getExpr(), *right->getExpr())));
        } else {
            expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_ge(*context, *left->getExpr(), *right->getExpr())));
        }
        return Z3ExprAndFieldMap::create(expr, leftFieldMap);
    } else if (expression->instanceOf<EqualsExpressionNode>()) {
        auto equalsExpressionNode = expression->as<EqualsExpressionNode>();
        auto left = createForExpression(equalsExpressionNode->getLeft(), context);
        auto right = createForExpression(equalsExpressionNode->getRight(), context);

        //Merge the right field map into left field map
        auto leftFiledMap = left->getFieldMap();
        leftFiledMap.merge(right->getFieldMap());

        auto expr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, *left->getExpr(), *right->getExpr())));
        return Z3ExprAndFieldMap::create(expr, leftFiledMap);
    } else if (expression->instanceOf<NegateExpressionNode>()) {
        auto negateExpressionNode = expression->as<NegateExpressionNode>();
        auto expr = createForExpression(negateExpressionNode->child(), context);
        auto updatedExpr = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_not(*context, *expr->getExpr())));
        return Z3ExprAndFieldMap::create(updatedExpr, expr->getFieldMap());
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for the logical expression node: " + expression->toString());
    return nullptr;
}

}// namespace NES::Optimizer
