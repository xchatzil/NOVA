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

#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <climits>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

using namespace NES;
/*
 * This test checks for the correctness of the pattern queries created by the NESPL Parsing Service.
 */
class PatternParsingServiceTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }
};

std::string queryPlanToString(const QueryPlanPtr queryPlan) {
    std::regex r1("  ");
    std::regex r2("[0-9]");
    std::regex r3("\\(.*?\\$");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r3, "");
    return queryPlanStr;
}

TEST_F(PatternParsingServiceTest, simplePattern) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed < A.allowedSpeed INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);
    // expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode(),
                                                     NES::Attribute("allowedSpeed").getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternTwoFilters) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed < A.allowedSpeed && A.value > "
                                "A.random INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    // expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    LogicalOperatorPtr filter1 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode(),
                                                     NES::Attribute("allowedSpeed").getExpressionNode())));
    LogicalOperatorPtr filter2 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterExpressionNode::create(NES::Attribute("value").getExpressionNode(),
                                                        NES::Attribute("random").getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(source);
    queryPlan->appendOperatorAsNewRoot(filter2);
    queryPlan->appendOperatorAsNewRoot(filter1);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternWithMultipleSinks) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink, File :: testSink2, NullOutput :: testSink3  ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);
    // expected results
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    LogicalOperatorPtr sinkPrint = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    LogicalOperatorPtr sinkFile = LogicalOperatorFactory::createSinkOperator(NES::FileSinkDescriptor::create("testSink2"));
    LogicalOperatorPtr sinkNull = LogicalOperatorFactory::createSinkOperator(NES::NullOutputSinkDescriptor::create());
    sinkPrint->addChild(source);
    sinkFile->addChild(source);
    sinkNull->addChild(source);
    queryPlan->addRootOperator(sinkPrint);
    queryPlan->addRootOperator(sinkFile);
    queryPlan->addRootOperator(sinkNull);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, DisjunctionPattern) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString =
        "PATTERN test:= (A OR B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(source2);
    OperatorPtr unionOp = LogicalOperatorFactory::createUnionOperator();
    queryPlan->appendOperatorAsNewRoot(unionOp);
    unionOp->addChild(subQueryPlan->getRootOperators()[0]);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, ConjunctionPattern) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical_b AS B WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    queryPlan->addRootOperator(source1);
    subQueryPlan->addRootOperator(source2);
    NES::Query query = Query(queryPlan)
                           .andWith(Query(subQueryPlan))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)));
    queryPlan = query.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, ConjunctionPatternWithFilter) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical_b AS B  WHERE "
                                "A.currentSpeed < A.allowedSpeed WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(source2);
    NES::Query query = Query(queryPlan)
                           .andWith(Query(subQueryPlan))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = query.getQueryPlan();
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode(),
                                                     NES::Attribute("allowedSpeed").getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, SequencePattern) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A SEQ B) FROM default_logical AS A, default_logical_b AS B WITHIN [3 MINUTE] INTO Print :: testSink";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(source2);
    NES::Query qSEQ = NES::Query(queryPlan)
                          .seqWith(NES::Query(subQueryPlan))
                          .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qSEQ.getQueryPlan();
    LogicalOperatorPtr sink4 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink4);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternWithSelect) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A) FROM default_logical AS A SELECT ce := [id=TU] INTO Print :: testSink ";
    //TODO fix Lexer, remove filter condition from output, i.e., no TU #2933
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(source);
    std::vector<ExpressionNodePtr> expression;
    expression.push_back(Attribute("id").getExpressionNode());
    LogicalOperatorPtr projection = LogicalOperatorFactory::createProjectionOperator(expression);
    queryPlan->appendOperatorAsNewRoot(projection);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternWithMultipleSelectStatements) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A) FROM default_logical AS A SELECT ce := [name=TU, type=random, "
                                "department=DIMA ] INTO Print :: testSink ";
    //TODO fix Lexer, remove filter condition from output, i.e., no TU #2933
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(source);
    std::vector<ExpressionNodePtr> expression;
    expression.push_back(Attribute("name").getExpressionNode());
    expression.push_back(Attribute("type").getExpressionNode());
    expression.push_back(Attribute("department").getExpressionNode());
    LogicalOperatorPtr projection = LogicalOperatorFactory::createProjectionOperator(expression);
    queryPlan->appendOperatorAsNewRoot(projection);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperator) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A[2:10]) FROM default_logical AS A WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTimes = NES::Query(queryPlan).times(2, 10).window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTimes.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperatorExact) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A[2]) FROM default_logical AS A WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTime = NES::Query(queryPlan).times(2).window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTime.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperatorUnbounded) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A+) FROM default_logical AS A WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTimes = NES::Query(queryPlan).times().window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTimes.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternFail) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test:= ";
    std::string patternString2 = "";
    std::shared_ptr<QueryParsingService> patternParsingService;
    // expected result
    EXPECT_ANY_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString));
    EXPECT_ANY_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString2));
}

TEST_F(PatternParsingServiceTest, SimpleWhereConstantValueRight) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test := (A SEQ B) FROM default_logical AS A, default_logical_b AS B WHERE A.id >= 400 "
                                "WITHIN [3 MINUTE] INTO Print :: outStream";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    // create query plan for A
    QueryPlanPtr queryPlanA = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlanA->addRootOperator(source1);
    // create query plan for B
    QueryPlanPtr queryPlanB = QueryPlan::create();
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    // add SEQ and window
    queryPlanB->addRootOperator(source2);
    NES::Query qSEQ = NES::Query(queryPlanA)
                          .seqWith(NES::Query(queryPlanB))
                          .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlanA = qSEQ.getQueryPlan();
    // add filter for WHERE clause
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("id").getExpressionNode(),
                                                              NES::ExpressionItem(400).getExpressionNode())));
    queryPlanA->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlanA->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, FloatConstantValue) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test := (A SEQ B) FROM default_logical AS A, default_logical_b AS B WHERE A.id >= -30.5 "
                                "WITHIN [3 MINUTE] INTO Print :: outStream";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    // create query plan for A
    QueryPlanPtr queryPlanA = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlanA->addRootOperator(source1);
    // create query plan for B
    QueryPlanPtr queryPlanB = QueryPlan::create();
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    // add SEQ and window
    queryPlanB->addRootOperator(source2);
    NES::Query qSEQ = NES::Query(queryPlanA)
                          .seqWith(NES::Query(queryPlanB))
                          .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlanA = qSEQ.getQueryPlan();
    // add filter for WHERE clause
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("id").getExpressionNode(),
                                                              NES::ExpressionItem(-30.5).getExpressionNode())));
    queryPlanA->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlanA->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, failPatternWithinExpected) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test := (A AND B) FROM default_logical AS A, default_logical_b INTO Print :: outStream";
    std::string patternString2 = "PATTERN test := (A SEQ B) FROM default_logical AS A, default_logical_b INTO Print :: outStream";
    std::string patternString3 = "PATTERN test := (A[3:10]) FROM default_logical AS A, default_logical_b INTO Print :: outStream";
    std::shared_ptr<QueryParsingService> patternParsingService;
    // expected result
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString),
                 Exceptions::RuntimeException);
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString2),
                 Exceptions::RuntimeException);
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString3),
                 Exceptions::RuntimeException);
}
