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
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Types/WindowType.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

using namespace Configurations;

class QueryAPITest : public Testing::BaseUnitTest {
  public:
    PhysicalSourcePtr physicalSource;
    LogicalSourcePtr logicalSource;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto defaultSourceType = DefaultSourceType::create("test2", "test_source");
        physicalSource = PhysicalSource::create(defaultSourceType);
        logicalSource = LogicalSource::create("test2", Schema::create());
    }
};

TEST_F(QueryAPITest, testQueryFilter) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryProjection) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("id");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").project(Attribute("id"), Attribute("value")).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryTumblingWindow) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_TRUE(sinkOptr->getSinkDescriptor()->instanceOf<PrintSinkDescriptor>());
}

TEST_F(QueryAPITest, testQuerySlidingWindow) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(SlidingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10), Seconds(2)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Merge two input source: one with filter and one without filter.
 */
TEST_F(QueryAPITest, testQueryMerge) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").unionWith(subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());
    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * Join two input source: one with filter and one without filter.
 */
TEST_F(QueryAPITest, testQueryJoin) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);

    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("id") == Attribute("id"))
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());
    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

/**
 * CrossJoin with two input sources: one with filter and one without filter.
 */
TEST_F(QueryAPITest, testQueryCrossJoin) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto filterSubQuery = Query::from("default_logical").filter(lessExpression);

    auto crossJoinQuery = Query::from("default_logical")
                              .crossJoinWith(filterSubQuery)
                              .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                              .sink(printSinkDescriptor);
    auto plan = crossJoinQuery.getQueryPlan();
    auto sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 2U);
    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());
    auto sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);
    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);
}

TEST_F(QueryAPITest, testQueryExpression) {
    auto andExpression = Attribute("f1") && 10;
    EXPECT_TRUE(andExpression->instanceOf<AndExpressionNode>());

    auto orExpression = Attribute("f1") || 45;
    EXPECT_TRUE(orExpression->instanceOf<OrExpressionNode>());

    auto lessExpression = Attribute("f1") < 45;
    EXPECT_TRUE(lessExpression->instanceOf<LessExpressionNode>());

    auto lessThenExpression = Attribute("f1") <= 45;
    EXPECT_TRUE(lessThenExpression->instanceOf<LessEqualsExpressionNode>());

    auto equalsExpression = Attribute("f1") == 45;
    EXPECT_TRUE(equalsExpression->instanceOf<EqualsExpressionNode>());

    auto greaterExpression = Attribute("f1") > 45;
    EXPECT_TRUE(greaterExpression->instanceOf<GreaterExpressionNode>());

    auto greaterThenExpression = Attribute("f1") >= 45;
    EXPECT_TRUE(greaterThenExpression->instanceOf<GreaterEqualsExpressionNode>());

    auto notEqualExpression = Attribute("f1") != 45;
    EXPECT_TRUE(notEqualExpression->instanceOf<NegateExpressionNode>());
    auto equals = notEqualExpression->as<NegateExpressionNode>()->child();
    EXPECT_TRUE(equals->instanceOf<EqualsExpressionNode>());

    auto assignmentExpression = Attribute("f1") = --Attribute("f1")++ + 10;
    ConsoleDumpHandler::create(std::cout)->dump(assignmentExpression);
    EXPECT_TRUE(assignmentExpression->instanceOf<FieldAssignmentExpressionNode>());
}

/**
 * @brief Test if the custom field set for aggregation using "as()" is set in the sink output schema
 */
TEST_F(QueryAPITest, windowAggregationWithAs) {

    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, WorkerId(1));

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addPhysicalSource("default_logical", sce);

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    // create a query with "as" in the aggregation
    auto query =
        Query::from("default_logical")
            .window(TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)))
            .byKey(Attribute("id", BasicType::INT64))
            .apply(Sum(Attribute("value", BasicType::INT64))->as(FieldAccessExpressionNode::create("MY_OUTPUT_FIELD_NAME")))
            .filter(Attribute("MY_OUTPUT_FIELD_NAME") > 1)
            .sink(PrintSinkDescriptor::create());

    Catalogs::UDF::UDFCatalogPtr udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    // only perform type inference phase to check if the modified aggregation field name is set in the output schema of the sink
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // get the output schema of the sink
    const auto outputSchemaString = query.getQueryPlan()->getSinkOperators()[0]->getOutputSchema()->toString();
    NES_DEBUG("QueryExecutionTest:: WindowAggWithAs outputSchema: {}", outputSchemaString);

    EXPECT_THAT(outputSchemaString, ::testing::HasSubstr("MY_OUTPUT_FIELD_NAME"));
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTest) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorPtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator with minuium count
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTestWithMinSupport) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorPtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

/**
 * @brief Test if the system can create a logical query plan with a Threshold Window Operator and minium count
 */
TEST_F(QueryAPITest, ThresholdWindowQueryTestwithKeyAndMinCount) {

    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();

    // without by key
    auto query = Query::from("default_logical")
                     .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                     .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                     .sink(PrintSinkDescriptor::create());

    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators = plan->getSourceOperators();
    EXPECT_EQ(sourceOperators.size(), 1U);

    SourceLogicalOperatorPtr srcOptr = sourceOperators[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators = plan->getSinkOperators();
    EXPECT_EQ(sinkOperators.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr = sinkOperators[0];
    EXPECT_EQ(sinkOperators.size(), 1U);

    // with by key
    auto query2 = Query::from("default_logical")
                      .window(ThresholdWindow::of(Attribute("f1") < 45, 5))
                      .byKey(Attribute("id", BasicType::INT64))
                      .apply(Sum(Attribute("value", BasicType::INT64))->as(Attribute("MY_OUTPUT_FIELD_NAME")))
                      .sink(PrintSinkDescriptor::create());

    auto plan2 = query2.getQueryPlan();
    const std::vector<SourceLogicalOperatorPtr> sourceOperators2 = plan2->getSourceOperators();
    EXPECT_EQ(sourceOperators2.size(), 1U);

    SourceLogicalOperatorPtr srcOptr2 = sourceOperators2[0];
    EXPECT_TRUE(srcOptr->getSourceDescriptor()->instanceOf<LogicalSourceDescriptor>());

    const std::vector<SinkLogicalOperatorPtr> sinkOperators2 = plan2->getSinkOperators();
    EXPECT_EQ(sinkOperators2.size(), 1U);

    SinkLogicalOperatorPtr sinkOptr2 = sinkOperators2[0];
    EXPECT_EQ(sinkOperators2.size(), 1U);
}

/**
 * @brief Test for Sequence-Operator (seqWith) with two sources.
 * This test compares the structure of the seqWith operator with the structure of the joinWith operator.
 * Query: SEQ(A,B) WITHIN 2 minutes
 */
TEST_F(QueryAPITest, testQuerySeqWithTwoSources) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;
    auto subQueryB = Query::from("default_logical").filter(lessExpression);// B in query

    // Query: SEQ(A,B) WITHIN 2 minutes
    auto querySeq = Query::from("default_logical")// A in query
                        .seqWith(subQueryB)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .sink(PrintSinkDescriptor::create());
    // create query with joinWith-operator instead of seqWith for comparison
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    auto queryJoin = Query::from("default_logical")                   // A in query
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryB.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         .filter(Attribute("logical$timestamp") < Attribute("logical$timestamp"))
                         .sink(PrintSinkDescriptor::create());
    auto seqPlan = querySeq.getQueryPlan();
    auto joinPlan = queryJoin.getQueryPlan();
    // compare if seq- and join-plan are equal
    EXPECT_TRUE(seqPlan->compare(joinPlan));
}

/**
 * @brief Test for Sequence-Operator (seqWith) with three sources.
 * This test compares the structure of the seqWith operator with the structure of the joinWith operator.
 * Query: SEQ(A,B,C) WITHIN 2 minutes
 */
TEST_F(QueryAPITest, testQuerySeqWithThreeSources) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;
    auto subQueryB = Query::from("default_logical").filter(lessExpression);// B in query
    auto subQueryC = Query::from("default_logical").filter(lessExpression);// C in query

    // Query: SEQ(A,B,C) WITHIN 2 minutes
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    auto querySeq = Query::from("default_logical")                    // A in query
                        .seqWith(subQueryB)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .seqWith(subQueryC)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .sink(PrintSinkDescriptor::create());
    // reset input streams
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    subQueryC = Query::from("default_logical").filter(lessExpression);// reset C
    auto queryJoin = Query::from("default_logical")                   // A in query
                         // create seqWith B
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryB.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         .filter(Attribute("logical$timestamp") < Attribute("logical$timestamp"))
                         // create seqWith C
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryC.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         .filter(Attribute("logical$timestamp") < Attribute("logical$timestamp"))
                         .sink(PrintSinkDescriptor::create());
    auto seqPlan = querySeq.getQueryPlan();
    auto joinPlan = queryJoin.getQueryPlan();
    // compare if seq- and join-plan are equal
    EXPECT_TRUE(seqPlan->compare(joinPlan));
}

/**
 * @brief Test for AND-Operator (andWith) with two sources.
 * Compares the actual query plan of andWith with the expected query plan.
 * Query: AND(A,B) WITHIN 2 minutes
 */
TEST_F(QueryAPITest, testQueryAndWithTwoSources) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;
    auto subQueryB = Query::from("default_logical").filter(lessExpression);// B in query

    // Query: AND(A,B) WITHIN 2 minutes
    auto queryAnd = Query::from("default_logical")// A in query
                        .andWith(subQueryB)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .sink(PrintSinkDescriptor::create());
    // reset input streams
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    auto queryJoin = Query::from("default_logical")                   // A in query
                         // create andWith B
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryB.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         .sink(PrintSinkDescriptor::create());
    auto andPlan = queryAnd.getQueryPlan();
    auto joinPlan = queryJoin.getQueryPlan();
    // compare if and- and join-plan are equal
    EXPECT_TRUE(andPlan->compare(joinPlan));
}

/**
 * @brief Test for AND-Operator (andWith) with three sources.
 * Compares the actual query plan of andWith with the expected query plan.
 * Query: AND(A,B,C) WITHIN 2 minutes
 */
TEST_F(QueryAPITest, testQueryAndWithThreeSources) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;
    auto subQueryB = Query::from("default_logical").filter(lessExpression);// B in query
    auto subQueryC = Query::from("default_logical").filter(lessExpression);// C in query

    // Query: AND(A,B,C) WITHIN 2 minutes
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    auto queryAnd = Query::from("default_logical")                    // A in query
                        .andWith(subQueryB)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .andWith(subQueryC)
                        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                        .sink(PrintSinkDescriptor::create());
    // reset input streams
    subQueryB = Query::from("default_logical").filter(lessExpression);// reset B
    subQueryC = Query::from("default_logical").filter(lessExpression);// reset C
    auto queryJoin = Query::from("default_logical")                   // A in query
                         // create andWith B
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryB.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         // create andWith C
                         .map(Attribute("cep_leftKey") = 1)
                         .joinWith(subQueryC.map(Attribute("cep_rightKey") = 1))
                         .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                         .sink(PrintSinkDescriptor::create());
    auto andPlan = queryAnd.getQueryPlan();
    auto joinPlan = queryJoin.getQueryPlan();
    // compare if and- and join-plan are equal
    EXPECT_TRUE(andPlan->compare(joinPlan));
}

/**
 * @brief Test for Times-Operator.
 * In this test, there are no occurrences specified (Times() without parameter),
 * leading to a query that expects the default behavior of the Times-operator without filtering for min/max occurrences.
 */
TEST_F(QueryAPITest, testQueryTimesNoSpecifiedOccurrences) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;

    auto timesQuery = Query::from("default_logical")
                          .times()
                          .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                          .sink(PrintSinkDescriptor::create());
    auto expectedQuery = Query::from("default_logical")
                             .map(Attribute("Count") = 1)
                             .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                             .apply(API::Sum(Attribute("Count")), API::Max(Attribute("timestamp")))
                             .sink(PrintSinkDescriptor::create());
    auto timesPlan = timesQuery.getQueryPlan();
    auto expectedPlan = expectedQuery.getQueryPlan();
    // compare if times- and expected-plan are equal
    EXPECT_TRUE(timesPlan->compare(expectedPlan));
}

/**
 * @brief Test for Times-Operator.
 * In this test, there is an exact number (larger than 0) occurrences specified (Times(occurrences)),
 * leading to a query that filters the tuples by the exact number of occurrences.
 */
TEST_F(QueryAPITest, testQueryTimesExactOccurrences) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;

    uint64_t numberOfOccurrences = 3;

    auto timesQuery = Query::from("default_logical")
                          .times(numberOfOccurrences)
                          .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                          .sink(PrintSinkDescriptor::create());
    auto expectedQuery = Query::from("default_logical")
                             .map(Attribute("Count") = 1)
                             .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                             .apply(API::Sum(Attribute("Count")), API::Max(Attribute("timestamp")))
                             // filter by exact number of occurrences
                             .filter(Attribute("Count") == numberOfOccurrences)
                             .sink(PrintSinkDescriptor::create());
    auto timesPlan = timesQuery.getQueryPlan();
    auto expectedPlan = expectedQuery.getQueryPlan();
    // compare if times- and expected-plan are equal
    EXPECT_TRUE(timesPlan->compare(expectedPlan));
}

/**
* @brief Test for Times-Operator.
* If the exact number of occurrences is 0 (Times(0)), this test should fail because
* min- and max-occurrences are set to 0 which is not allowed.
*/
TEST_F(QueryAPITest, testQueryTimesZeroOccurrences) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;

    uint64_t numberOfOccurrences = 0;
    try {
        auto timesQuery = Query::from("default_logical")
                              .times(numberOfOccurrences)
                              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                              .sink(PrintSinkDescriptor::create());
    } catch (const NES::Exceptions::RuntimeException& error) {
        // expected to fail with a RuntimeException since numberOfOccurrences == 0 is not allowed.
        EXPECT_THAT(error.what(), ::testing::HasSubstr("Number of occurrences must be at least 1."));
    } catch (...) {
        FAIL() << "Expected NES::Exceptions::RuntimeException to be thrown, because occurrences == 0 is not allowed.";
    }
}

/**
 * @brief Test for Times-Operator.
 * If there are two parameters specified (Times(min_occurrences, max_occurrences)), the query-plan
 * is expected to filtered according to the minimum and maximum number of occurrences.
 */
TEST_F(QueryAPITest, testQueryTimesMinMaxOccurrences) {
    auto schema = TestSchemas::getSchemaTemplate("id_val_u64");
    auto lessExpression = Attribute("field_1") <= 10;

    uint64_t numberOfMinOccurrences = 3;
    uint64_t numberOfMaxOccurrences = 5;

    auto timesQuery = Query::from("default_logical")
                          .times(numberOfMinOccurrences, numberOfMaxOccurrences)
                          .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                          .sink(PrintSinkDescriptor::create());
    auto expectedQuery = Query::from("default_logical")
                             .map(Attribute("Count") = 1)
                             .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(2)))
                             .apply(API::Sum(Attribute("Count")), API::Max(Attribute("timestamp")))
                             // filter by min-/max-occurrences
                             .filter(Attribute("Count") >= numberOfMinOccurrences && Attribute("Count") <= numberOfMaxOccurrences)
                             .sink(PrintSinkDescriptor::create());
    auto timesPlan = timesQuery.getQueryPlan();
    auto expectedPlan = expectedQuery.getQueryPlan();
    // compare if times- and expected-plan are equal
    EXPECT_TRUE(timesPlan->compare(expectedPlan));
}

}// namespace NES
