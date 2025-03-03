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

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
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
#include <Services/QueryParsingService.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <stdint.h>

namespace NES {

class ExpressionNodeTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    static void SetUpTestCase() { setupLogging(); }

  protected:
    static void setupLogging() {
        NES::Logger::setupLogging("ExpressionNodeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionNodeTest test class.");
    }
};

TEST_F(ExpressionNodeTest, predicateConstruction) {
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    ASSERT_FALSE(left->isPredicate());
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "11"));
    ASSERT_FALSE(right->isPredicate());
    auto expression = EqualsExpressionNode::create(left, right);

    // check if expression is a predicate
    EXPECT_TRUE(expression->isPredicate());
    auto fieldRead = FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "field_1");
    auto constant = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, "10"));
    auto lessThen = LessEqualsExpressionNode::create(fieldRead, constant);
    EXPECT_TRUE(lessThen->isPredicate());

    auto andExpression = AndExpressionNode::create(expression, lessThen);
    ConsoleDumpHandler::create(std::cout)->dump(andExpression);
    EXPECT_TRUE(andExpression->isPredicate());
}

TEST_F(ExpressionNodeTest, attributeStampInference) {
    auto schema = Schema::create()->addField("test$f1", BasicType::INT8);

    auto attribute = Attribute("f1").getExpressionNode();
    // check if the attribute field is initially undefined
    EXPECT_TRUE(attribute->getStamp()->isUndefined());

    // infer stamp using schema
    attribute->inferStamp(schema);
    EXPECT_TRUE(attribute->getStamp()->equals(DataTypeFactory::createInt8()));

    // test inference with undefined attribute
    auto notValidAttribute = Attribute("f2").getExpressionNode();

    EXPECT_TRUE(notValidAttribute->getStamp()->isUndefined());
    // we expect that this call throws an exception
    ASSERT_ANY_THROW(notValidAttribute->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferenceExpressionTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", BasicType::INT8)
                      ->addField("test$f2", BasicType::INT64)
                      ->addField("test$f3", BasicType::FLOAT64)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto addExpression = Attribute("f1") + 10;
    EXPECT_TRUE(addExpression->getStamp()->isUndefined());
    addExpression->inferStamp(schema);
    EXPECT_TRUE(addExpression->getStamp()->equals(DataTypeFactory::createType(BasicType::INT32)));

    auto mulExpression = Attribute("f2") * 10;
    EXPECT_TRUE(mulExpression->getStamp()->isUndefined());
    mulExpression->inferStamp(schema);
    EXPECT_TRUE(mulExpression->getStamp()->equals(DataTypeFactory::createType(BasicType::INT64)));

    auto increment = Attribute("f3")++;
    EXPECT_TRUE(increment->getStamp()->isUndefined());
    increment->inferStamp(schema);
    EXPECT_TRUE(increment->getStamp()->equals(DataTypeFactory::createType(BasicType::FLOAT64)));

    // We expect that you can't increment an array
    auto incrementArray = Attribute("f4")++;
    EXPECT_TRUE(incrementArray->getStamp()->isUndefined());
    ASSERT_ANY_THROW(incrementArray->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferPredicateTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", BasicType::INT8)
                      ->addField("test$f2", BasicType::INT64)
                      ->addField("test$f3", BasicType::BOOLEAN)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto equalsExpression = Attribute("f1") == 10;
    equalsExpression->inferStamp(schema);
    EXPECT_TRUE(equalsExpression->isPredicate());

    auto lessExpression = Attribute("f2") < 10;
    lessExpression->inferStamp(schema);
    EXPECT_TRUE(lessExpression->isPredicate());

    auto negateBoolean = !Attribute("f3");
    negateBoolean->inferStamp(schema);
    EXPECT_TRUE(negateBoolean->isPredicate());

    // you cant negate non boolean.
    auto negateInteger = !Attribute("f1");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));

    auto andExpression = Attribute("f3") && true;
    andExpression->inferStamp(schema);
    EXPECT_TRUE(andExpression->isPredicate());

    auto orExpression = Attribute("f3") || Attribute("f3");
    orExpression->inferStamp(schema);
    EXPECT_TRUE(orExpression->isPredicate());
    // you cant make a logical expression between non boolean.
    auto orIntegerExpression = Attribute("f1") || Attribute("f2");
    ASSERT_ANY_THROW(negateInteger->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, inferAssertionTest) {
    auto schema = Schema::create()
                      ->addField("test$f1", BasicType::INT8)
                      ->addField("test$f2", BasicType::INT64)
                      ->addField("test$f3", BasicType::BOOLEAN)
                      ->addField("test$f4", DataTypeFactory::createArray(10, DataTypeFactory::createBoolean()));

    auto assertion = Attribute("f1") = 10 * (33 + Attribute("f1"));
    assertion->inferStamp(schema);
    EXPECT_TRUE(assertion->getField()->getStamp()->equals(DataTypeFactory::createType(BasicType::INT8)));
}

TEST_F(ExpressionNodeTest, multiplicationInferStampTest) {
    auto schema = Schema::create()->addField("test$left", BasicType::UINT32)->addField("test$right", BasicType::INT16);

    auto multiplicationNode = Attribute("left") * Attribute("right");
    multiplicationNode->inferStamp(schema);
    ASSERT_TRUE(multiplicationNode->getStamp()->isInteger());
    auto intStamp = DataType::as<Integer>(multiplicationNode->getStamp());
    int bits = intStamp->getBits();
    int64_t lowerBound = intStamp->lowerBound;
    int64_t upperBound = intStamp->upperBound;
    EXPECT_TRUE(bits == 32);
    EXPECT_TRUE(lowerBound == INT16_MIN);
    EXPECT_TRUE(upperBound == UINT32_MAX);
    //TODO: question about the bits in stamps: Does this result of our stamp inference make sense? A 32 bit integer can not store all values between [INT16_MIN,UINT32_MAX]
}

/**
 * @brief Test behaviour of special ModExpressionNode::inferStamp function. (integers)
 */
TEST_F(ExpressionNodeTest, moduloIntegerInferStampTest) {
    auto schema = Schema::create()->addField("test$left", BasicType::UINT32)->addField("test$right", BasicType::INT16);

    auto moduloNode = MOD(Attribute("left"), Attribute("right"));
    moduloNode->inferStamp(schema);
    ASSERT_TRUE(moduloNode->getStamp()->isInteger());

    auto intStamp = DataType::as<Integer>(moduloNode->getStamp());
    int bits = intStamp->getBits();
    int64_t lowerBound = intStamp->lowerBound;
    int64_t upperBound = intStamp->upperBound;
    NES_INFO("{}", upperBound);
    EXPECT_EQ(bits, 32);
    EXPECT_EQ(lowerBound, 0);
    EXPECT_EQ(
        upperBound,
        -INT16_MIN
            - 1);// e.g. when calculating MOD(..., -128) the result will always be in range [-127, 127]. And no other INT8 divisor will yield a wider range than -128 (=INT8_MIN).
    EXPECT_EQ(upperBound, INT16_MAX);// equivalent to above
};

/**
 * @brief Test behaviour of special ModExpressionNode::inferStamp function. (float)
 */
TEST_F(ExpressionNodeTest, moduloFloatInferStampTest) {
    auto schema = Schema::create()->addField("test$left", BasicType::UINT32)->addField("test$right", BasicType::FLOAT32);

    auto moduloNode = MOD(Attribute("left"), Attribute("right"));
    moduloNode->inferStamp(schema);
    ASSERT_TRUE(moduloNode->getStamp()->isFloat());

    auto floatStamp = DataType::as<Float>(moduloNode->getStamp());
    int bits = floatStamp->getBits();
    int64_t lowerBound = floatStamp->lowerBound;
    int64_t upperBound = floatStamp->upperBound;
    NES_INFO("{}", upperBound);
    EXPECT_EQ(bits, 32);
    EXPECT_EQ(lowerBound, 0);         // == lower bound of UINT32, as it is is higher than range spanned by Float divisor
    EXPECT_EQ(upperBound, UINT32_MAX);// == upper bound of UINT32, as it  is lower than range spanned by Float divisor
}

/**
 * @brief Test behaviour of special WhenExpressionNode::inferStamp function. (float)
 */
TEST_F(ExpressionNodeTest, whenInferStampTest) {
    auto schema = Schema::create()->addField("test$bool", BasicType::BOOLEAN)->addField("test$float", BasicType::FLOAT32);
    auto whenNode = WHEN(Attribute("bool"), Attribute("float"));
    ASSERT_TRUE(whenNode->getStamp()->isUndefined());
    whenNode->inferStamp(schema);
    ASSERT_TRUE(whenNode->getStamp()->isFloat());
}

/**
 * @brief Test behaviour of special CaseExpressionNode::inferStamp function.
 */
TEST_F(ExpressionNodeTest, caseInfereStampTest) {
    auto schema = Schema::create()
                      ->addField("test$bool1", BasicType::BOOLEAN)
                      ->addField("test$bool2", BasicType::BOOLEAN)
                      ->addField("test$float1", BasicType::FLOAT32)
                      ->addField("test$float2", BasicType::FLOAT32)
                      ->addField("test$float3", BasicType::FLOAT32)
                      ->addField("test$integer", BasicType::INT32);

    auto whenNode1 = WHEN(Attribute("bool1"), Attribute("float1"));
    auto whenNode2 = WHEN(Attribute("bool2"), Attribute("float2"));

    //test expected use
    auto caseNode = CASE({whenNode1, whenNode2}, Attribute("float3"));
    ASSERT_TRUE(caseNode->getStamp()->isUndefined());
    caseNode->inferStamp(schema);
    ASSERT_TRUE(caseNode->getStamp()->isFloat());

    //test error-throwing-use, by mixing stamps of whens-expressions
    //different stamp of default-expression
    auto badCaseNode1 = CASE({whenNode1, whenNode2}, Attribute("integer"));

    //when node with integer
    auto whenNode3 = WHEN(Attribute("bool1"), Attribute("integer"));
    // different stamp of integer when-expression whenNode3
    auto badCaseNode2 = CASE({whenNode1, whenNode3}, Attribute("float3"));
    ASSERT_ANY_THROW(badCaseNode1->inferStamp(schema));
    ASSERT_ANY_THROW(badCaseNode2->inferStamp(schema));
}

TEST_F(ExpressionNodeTest, testOrExpressionNodeEqual) {
    auto expr1 = Attribute("f1") < 10 || Attribute("f2") > 12;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") < 10 || Attribute("f2") > 13;
    auto expr4 = Attribute("f2") < 10 || Attribute("f2") > 12;

    ASSERT_TRUE(expr1->instanceOf<OrExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<OrExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<OrExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testAndExpressionNodeEqual) {
    auto expr1 = Attribute("f1") > 10 && Attribute("f2") < 12;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") > 10 && Attribute("f2") < 13;
    auto expr4 = Attribute("f2") > 10 && Attribute("f2") < 12;

    ASSERT_TRUE(expr1->instanceOf<AndExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<AndExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<AndExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testEqualsExpressionNodeEqual) {
    auto expr1 = Attribute("f1") == 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") == 12;
    auto expr4 = Attribute("f2") == 10;

    ASSERT_TRUE(expr1->instanceOf<EqualsExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<EqualsExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<EqualsExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testNegateExpressionNodeEqual) {
    auto expr1 = Attribute("f1") != 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") != 12;
    auto expr4 = Attribute("f2") != 10;

    ASSERT_TRUE(expr1->instanceOf<NegateExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<NegateExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<NegateExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testLessEqualsExpressionNodeEqual) {
    auto expr1 = Attribute("f1") <= 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") <= 12;
    auto expr4 = Attribute("f2") <= 10;

    ASSERT_TRUE(expr1->instanceOf<LessEqualsExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<LessEqualsExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<LessEqualsExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testLessExpressionNodeEqual) {
    auto expr1 = Attribute("f1") < 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") < 12;
    auto expr4 = Attribute("f2") < 10;

    ASSERT_TRUE(expr1->instanceOf<LessExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<LessExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<LessExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testGreaterEqualsExpressionNodeEqual) {
    auto expr1 = Attribute("f1") >= 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") >= 12;
    auto expr4 = Attribute("f2") >= 10;

    ASSERT_TRUE(expr1->instanceOf<GreaterEqualsExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<GreaterEqualsExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<GreaterEqualsExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

TEST_F(ExpressionNodeTest, testGreaterExpressionNodeEqual) {
    auto expr1 = Attribute("f1") > 10;
    auto expr2 = expr1->copy();
    auto expr3 = Attribute("f1") > 12;
    auto expr4 = Attribute("f2") > 10;

    ASSERT_TRUE(expr1->instanceOf<GreaterExpressionNode>());
    ASSERT_TRUE(expr3->instanceOf<GreaterExpressionNode>());
    ASSERT_TRUE(expr4->instanceOf<GreaterExpressionNode>());
    ASSERT_TRUE(expr1->equal(expr2));
    ASSERT_FALSE(expr1->equal(expr3));
    ASSERT_FALSE(expr1->equal(expr4));
}

}// namespace NES
