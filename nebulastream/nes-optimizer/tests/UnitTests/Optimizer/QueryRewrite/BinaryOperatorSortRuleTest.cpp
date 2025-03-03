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

// clang-format off
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class BinaryOperatorSortRuleTest : public Testing::BaseUnitTest {

  public:
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BinaryOperatorSortRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BinaryOperatorSortRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup BinaryOperatorSortRuleTest test case.");
        auto schema1 = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64)
                           ->addField("ts", BasicType::UINT64);
        auto schema2 = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64)
                           ->addField("ts", BasicType::UINT64);
        sourceCatalog->addLogicalSource("src1", schema1);
        sourceCatalog->addLogicalSource("src2", schema2);
    }
};

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForUnionWithUnSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src1");
    Query query = Query::from("src2").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);

    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[1]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForUnionWithSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");
    Query query = Query::from("src1").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[0]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[1]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinWithUnSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src1");
    Query query = Query::from("src2")
                      .joinWith(subQuery)
                      .where(Attribute("id") == Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto joinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedJoinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[1]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinWithSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");
    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id") == Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto joinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedJoinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[0]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[1]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinAnUnionWithUnSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("src1");
    Query subQuery2 = Query::from("src1");
    Query query = Query::from("src2")
                      .unionWith(subQuery1)
                      .joinWith(subQuery2)
                      .where(Attribute("id") == Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    auto joinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[1]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[0]);

    auto updatedJoinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[1]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinAndUnionWithSortedChildren) {

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("src2");
    Query subQuery2 = Query::from("src2");
    Query query = Query::from("src1")
                      .unionWith(subQuery1)
                      .joinWith(subQuery2)
                      .where(Attribute("id") == Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    auto joinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[0]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[1]);

    auto updatedJoinOperators = queryPlan->getOperatorByType<LogicalJoinOperator>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[0]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[1]);
}
