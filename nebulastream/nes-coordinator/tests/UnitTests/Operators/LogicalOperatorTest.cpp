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

#include <BaseIntegrationTest.hpp>//
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/DefaultSource.hpp>
#include <iostream>
#include <memory>

#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Util/Core.hpp>

using namespace std;
namespace NES {

class LogicalOperatorTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LogicalOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LogicalOperatorTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));

        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        logicalSource = sourceCatalog->getLogicalSourceOrThrowException("default_logical");
        SchemaPtr schema = logicalSource->getSchema();
        auto sourceDescriptor = DefaultSourceDescriptor::create(schema, /*number of buffers*/ 0, /*frequency*/ 0);

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));

        sourceOp = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5 = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6 = LogicalOperatorFactory::createFilterOperator(pred6);
        filterOp7 = LogicalOperatorFactory::createFilterOperator(pred7);

        filterOp1Copy = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2Copy = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3Copy = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4Copy = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5Copy = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6Copy = LogicalOperatorFactory::createFilterOperator(pred6);

        removed = false;
        replaced = false;
        children.clear();
        parents.clear();
    }

  protected:
    bool removed{};
    bool replaced{};
    LogicalSourcePtr logicalSource;
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorPtr sourceOp;

    LogicalOperatorPtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorPtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};
};

TEST_F(LogicalOperatorTest, getSuccessors) {

    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp2);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);

    children.push_back(filterOp3);
    EXPECT_EQ(children.size(), 3U);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);
}

TEST_F(LogicalOperatorTest, getPredecessors) {
    filterOp3->addParent(filterOp1);

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    parents.push_back(filterOp2);
    EXPECT_EQ(parents.size(), 2U);

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);
}

TEST_F(LogicalOperatorTest, addSelfAsSuccessor) {
    sourceOp->addChild(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);

    sourceOp->addChild(sourceOp);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
}

TEST_F(LogicalOperatorTest, addAndRemoveSingleSuccessor) {
    sourceOp->addChild(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveMultipleSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp2);
    sourceOp->addChild(filterOp3);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 3U);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeChild(filterOp2);
    EXPECT_TRUE(removed);
    removed = sourceOp->removeChild(filterOp3);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveDuplicatedSuccessors) {
    sourceOp->addChild(filterOp1);
    OperatorPtr duplicateFilterOp1 = filterOp1->duplicate();
    sourceOp->addChild(duplicateFilterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0U);

    removed = sourceOp->removeChild(duplicateFilterOp1);
    EXPECT_FALSE(removed);
    sourceOp->addChild(filterOp1Copy);
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveLogicalDuplicateButDifferentOperatorAsSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp1Copy);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);

    removed = sourceOp->removeChild(filterOp1);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);

    removed = sourceOp->removeChild(filterOp1Copy);
    EXPECT_TRUE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveNullSuccessor) {
    // assertion fail due to nullptr
    sourceOp->addChild(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_FALSE(sourceOp->addChild(nullptr));
    removed = sourceOp->removeChild(nullptr);
    EXPECT_FALSE(removed);
}

TEST_F(LogicalOperatorTest, addSelfAsPredecessor) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp3->addParent(filterOp3);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);
}

TEST_F(LogicalOperatorTest, addAndRemoveSinglePredecessor) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveMultiplePredecessors) {
    filterOp3->addParent(filterOp1);
    filterOp3->addParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 2U);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    removed = filterOp3->removeParent(filterOp2);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveDuplicatedPredecessors) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    OperatorPtr duplicateFilterOp1 = filterOp1->duplicate();
    filterOp3->addParent(duplicateFilterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);

    removed = filterOp3->removeParent(duplicateFilterOp1);
    EXPECT_FALSE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveLogicallyDuplicatedButDifferentOperatorAsPredecessors) {
    filterOp3->addParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp3->addParent(filterOp1Copy);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 2U);

    removed = filterOp3->removeParent(filterOp1);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    removed = filterOp3->removeParent(filterOp1Copy);
    EXPECT_TRUE(removed);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);
}

TEST_F(LogicalOperatorTest, consistencyBetweenSuccessorPredecesorRelation1) {
    filterOp3->addParent(filterOp1);
    filterOp3->addParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 2U);
    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1U);
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1U);

    filterOp3->removeParent(filterOp1);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);

    children = filterOp1->getChildren();
    NES_DEBUG("children of filterOp1");
    for (auto&& op : children) {
        NES_DEBUG("{}", op->toString());
    }
    std::cout << "================================================================================\n";
    EXPECT_EQ(children.size(), 0U);

    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1U);

    filterOp3->removeParent(filterOp2);
    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 0U);

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 0U);
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, consistencyBetweenSuccessorPredecesorRelation2) {
    filterOp3->addChild(filterOp1);
    filterOp3->addChild(filterOp2);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 2U);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1U);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp3->removeChild(filterOp1);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 1U);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0U);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp3->removeChild(filterOp2);
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0U);
    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 0U);
}

TEST_F(LogicalOperatorTest, addAndRemoveNullPredecessor) {
    // assertion failed due to nullptr
    EXPECT_TRUE(filterOp3->addParent(filterOp1));
    EXPECT_FALSE(filterOp3->addParent(nullptr));
    EXPECT_FALSE(filterOp3->removeParent(nullptr));
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 *           filterOp3 -> filterOp4
 * replaced: sourceOp -> filterOp3 -> filterOp2
 *                                |-> filterOp4
 */
TEST_F(LogicalOperatorTest, replaceSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);
    filterOp3->addChild(filterOp4);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp3, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp3));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 2U);
    EXPECT_TRUE(children[0]->equal(filterOp4));
    EXPECT_TRUE(children[1]->equal(filterOp2));
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */

TEST_F(LogicalOperatorTest, replaceWithEqualSuccessor) {
    children = filterOp1Copy->getChildren();
    EXPECT_EQ(children.size(), 0u);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp1Copy, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1u);

    children = filterOp1Copy->getChildren();
    EXPECT_EQ(children.size(), 1u);
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 *                   |-> filterOp3 -> filterOp4
 * replaced: sourceOp -> filterOp1 -> filterOp2
 *                   |-> filterOp3 -> filterOp4
 */

TEST_F(LogicalOperatorTest, replaceWithExistedSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp3->addChild(filterOp4);

    replaced = sourceOp->replace(filterOp3, filterOp1);
    EXPECT_FALSE(replaced);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);
    EXPECT_TRUE(children[0]->equal(filterOp1));
    EXPECT_TRUE(children[1]->equal(filterOp3));

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp2));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp4));
}

/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorTest, replaceWithIdenticalSuccessor) {
    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp1, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);

    children = filterOp1->getChildren();
    EXPECT_EQ(children.size(), 1U);
}

/**
 * @brief replace filterOp1 with filterOp2
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp2
 */
TEST_F(LogicalOperatorTest, replaceWithSubSuccessor) {
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp2, filterOp1);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp2));

    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

/**
 * @brief replace filterOp3 (not existed) with filterOp3
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorTest, replaceWithNoneSuccessor) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    sourceOp->replace(filterOp3, filterOp3);

    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp1));

    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);
}

TEST_F(LogicalOperatorTest, replaceSuccessorInvalidOldOperator) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    EXPECT_FALSE(sourceOp->replace(filterOp3, nullptr));
}

TEST_F(LogicalOperatorTest, replaceWithWithInvalidNewOperator) {
    children = filterOp3->getChildren();
    EXPECT_EQ(children.size(), 0U);

    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);

    EXPECT_FALSE(sourceOp->replace(nullptr, filterOp1));
}

/**
 * @brief replace filterOp1 with filterOp3
 * original: sourceOp <- filterOp1 <- filterOp2
 * replaced: sourceOp <- filterOp3 <- filterOp2
 */
TEST_F(LogicalOperatorTest, replacePredecessor) {
    parents = filterOp3->getParents();

    EXPECT_EQ(parents.size(), 0U);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    filterOp2->replace(filterOp3, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1U);
    EXPECT_EQ(parents[0].get(), filterOp3.get());

    parents = filterOp3->getParents();
    EXPECT_EQ(parents.size(), 1U);
}

/**
 * @brief replace filterOp1 with filterOp1Copy
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1Copy -> filterOp2
 */
TEST_F(LogicalOperatorTest, replaceWithEqualPredecessor) {
    parents = filterOp1Copy->getParents();
    EXPECT_EQ(parents.size(), 0U);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp2->replace(filterOp1Copy, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1U);
    EXPECT_NE(parents[0].get(), filterOp1.get());
    EXPECT_EQ(parents[0].get(), filterOp1Copy.get());

    parents = filterOp1Copy->getParents();
    EXPECT_EQ(parents.size(), 1U);
    EXPECT_EQ(parents[0].get(), sourceOp.get());
}
/**
 * @brief replace filterOp1 with filterOp1
 * original: sourceOp -> filterOp1 -> filterOp2
 * replaced: sourceOp -> filterOp1 -> filterOp2
 */
TEST_F(LogicalOperatorTest, replaceWithIdenticalPredecessor) {
    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 0U);

    filterOp2->addParent(filterOp1);
    filterOp1->addParent(sourceOp);

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1U);

    filterOp2->replace(filterOp1, filterOp1);

    parents = filterOp2->getParents();
    EXPECT_EQ(parents.size(), 1U);
    EXPECT_EQ(parents[0].get(), filterOp1.get());

    parents = filterOp1->getParents();
    EXPECT_EQ(parents.size(), 1U);
    EXPECT_EQ(parents[0].get(), sourceOp.get());
}

/**
 * @brief remove filterOp1 from topology
 * original: sourceOp -> filterOp1 -> filterOp2
 *                                |-> filterOp3
 * desired: sourceOp -> filterOp2
 *                  |-> filterOp3
 */
TEST_F(LogicalOperatorTest, removeExistedAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);

    sourceOp->removeAndLevelUpChildren(filterOp1);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);
    EXPECT_TRUE(children[0]->equal(filterOp2));
    EXPECT_TRUE(children[1]->equal(filterOp3));
}

/**
 * @brief remove filterOp4 from topology
 * original: sourceOp -> filterOp1 -> filterOp2
 *                                |-> filterOp3
 *
 * desired: sourceOp -> filterOp1 -> filterOp2
 *                               |-> filterOp3
 */
TEST_F(LogicalOperatorTest, removeNotExistedAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);

    removed = sourceOp->removeAndLevelUpChildren(filterOp4);
    EXPECT_FALSE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_TRUE(children[0]->equal(filterOp1));
}

/**
 * @brief remove filterOp1 from topology
 *
 *                                /-> filterOp3'
 * original: sourceOp -> filterOp1 -> filterOp2
 *                   \-> filterOp3
 *
 * desired:  unchanged
 *
 *
 */
TEST_F(LogicalOperatorTest, removeExistedSblingAndLevelUpSuccessors) {
    sourceOp->addChild(filterOp1);
    sourceOp->addChild(filterOp3);

    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3Copy);

    removed = sourceOp->removeAndLevelUpChildren(filterOp1);
    EXPECT_FALSE(removed);
    children = sourceOp->getChildren();
    EXPECT_EQ(children.size(), 2U);
    EXPECT_TRUE(children[0]->equal(filterOp1));
    EXPECT_TRUE(children[1]->equal(filterOp3));
}

TEST_F(LogicalOperatorTest, remove) {
    filterOp2->addParent(filterOp1);
    // filterOp3 neither in filterOp2's children nor parents
    removed = filterOp2->remove(filterOp3);
    EXPECT_FALSE(removed);
    // filterOp1 in filterOp2's parents
    removed = filterOp2->remove(filterOp1);
    EXPECT_TRUE(removed);

    filterOp2->addChild(filterOp3);
    removed = filterOp2->remove(filterOp3);
    EXPECT_TRUE(removed);
}

TEST_F(LogicalOperatorTest, clear) {
    filterOp2->addParent(filterOp1);
    filterOp2->addChild(filterOp3);
    parents = filterOp2->getParents();
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 1U);
    EXPECT_EQ(parents.size(), 1U);

    filterOp2->clear();
    parents = filterOp2->getParents();
    children = filterOp2->getChildren();
    EXPECT_EQ(children.size(), 0U);
    EXPECT_EQ(parents.size(), 0U);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *
 *                                    /-> filterOp4'
 * topology2: filterOp6' -> filterOp1' -> filterOp2'
 *                      \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorTest, equalWithAllSuccessors1) {
    bool same = false;

    EXPECT_TRUE(filterOp1->equal(filterOp1Copy));
    EXPECT_TRUE(filterOp2->equal(filterOp2Copy));
    EXPECT_TRUE(filterOp3->equal(filterOp3Copy));
    EXPECT_TRUE(filterOp4->equal(filterOp4Copy));
    EXPECT_TRUE(filterOp6->equal(filterOp6Copy));

    // topology1
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology2
    filterOp6Copy->addChild(filterOp1Copy);
    filterOp6Copy->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp4Copy);
    filterOp1Copy->addChild(filterOp2Copy);

    same = filterOp6->equalWithAllChildren(filterOp6Copy);
    EXPECT_TRUE(same);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *                                  /-> filterOp4'
 * topology3: sourceOp -> filterOp1' -> filterOp2'
 *                     \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorTest, equalWithAllSuccessors2) {
    bool same = true;
    // topology1
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp3);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology3
    sourceOp->addChild(filterOp1Copy);
    sourceOp->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp4Copy);
    filterOp1Copy->addChild(filterOp2Copy);

    same = filterOp6->equalWithAllChildren(sourceOp);
    EXPECT_FALSE(same);
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3
 *
 *                                    /-> filterOp5'
 * topology4: filterOp6' -> filterOp1' -> filterOp2'
 *                      \-> filterOp3'
 *
 */
TEST_F(LogicalOperatorTest, equalWithAllSuccessors3) {
    bool same = true;
    // topology1
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    // topology4
    filterOp6Copy->addChild(filterOp1Copy);
    filterOp6Copy->addChild(filterOp3Copy);
    filterOp1Copy->addChild(filterOp2Copy);
    filterOp1Copy->addChild(filterOp5Copy);

    same = filterOp6->equalWithAllChildren(filterOp6Copy);
    EXPECT_FALSE(same);
}

/**
 *
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *
 *                                    /<- filterOp4'
 * topology2: filterOp6' <- filterOp1' <- filterOp2'
 *                      \<- filterOp3'
 *
 */
TEST_F(LogicalOperatorTest, equalWithAllPredecessors1) {
    bool same = false;

    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology2
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp4Copy->addParent(filterOp1Copy);
    filterOp3Copy->addParent(filterOp6Copy);
    filterOp1Copy->addParent(filterOp6Copy);

    same = filterOp4->equalWithAllParents(filterOp4Copy);
    EXPECT_TRUE(same);
    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_TRUE(same);
    same = filterOp3->equalWithAllParents(filterOp3Copy);
    EXPECT_TRUE(same);
}

/**
 *
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *                                  /<- filterOp4'
 * topology3: sourceOp <- filterOp1' <- filterOp2'
 *                     \<- filterOp3'
 *
 */
TEST_F(LogicalOperatorTest, equalWithAllPredecessors2) {
    bool same = false;

    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology3
    filterOp4Copy->addParent(filterOp1Copy);
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp1Copy->addParent(sourceOp);
    filterOp3Copy->addParent(sourceOp);

    same = filterOp4->equalWithAllParents(filterOp4Copy);
    EXPECT_FALSE(same);
    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_FALSE(same);
    same = filterOp3->equalWithAllParents(filterOp3Copy);
    EXPECT_FALSE(same);
}

/**
 *                                  /<- filterOp4
 * topology1: filterOp6 <- filterOp1 <- filterOp2
 *                     \<- filterOp3
 *
 *
 *
 *                                    /<- filterOp5'
 * topology4: filterOp6' <- filterOp1' <- filterOp2'
 *                      \<- filterOp3'
 */
TEST_F(LogicalOperatorTest, equalWithAllPredecessors3) {
    bool same = false;
    // topology1
    filterOp2->addParent(filterOp1);
    filterOp4->addParent(filterOp1);
    filterOp1->addParent(filterOp6);
    filterOp3->addParent(filterOp6);

    // topology4
    filterOp5Copy->addParent(filterOp1Copy);
    filterOp2Copy->addParent(filterOp1Copy);
    filterOp3Copy->addParent(filterOp6Copy);
    filterOp1Copy->addParent(filterOp6Copy);

    same = filterOp5->equalWithAllParents(filterOp4Copy);
    EXPECT_FALSE(same);

    same = filterOp2->equalWithAllParents(filterOp2Copy);
    EXPECT_TRUE(same);
}

// TODO: add more operator casting
TEST_F(LogicalOperatorTest, as) {
    NodePtr base2 = filterOp1;
    LogicalFilterOperatorPtr _filterOp1 = base2->as<LogicalFilterOperator>();
}

TEST_F(LogicalOperatorTest, asBadCast) {
    NodePtr base2 = sourceOp;
    try {
        LogicalFilterOperatorPtr _filterOp1 = base2->as<LogicalFilterOperator>();
        FAIL();
    } catch (const std::exception& e) {
        SUCCEED();
    }
}

/**
 *
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *                     \-> filterOp3 -> filterOp5
 *
 */
TEST_F(LogicalOperatorTest, findRecurisivelyOperatorNotExists) {

    // topology1
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp3->addChild(filterOp5);

    NodePtr x = nullptr;
    // case 1: filterOp7 not in this graph
    x = Node::findRecursively(filterOp6, filterOp7);
    EXPECT_TRUE(x == nullptr);
    // case 2: filterOp6 is in this graph, but not the
    // children of filterOp1
    x = Node::findRecursively(filterOp1, filterOp6);
    EXPECT_TRUE(x == nullptr);
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorTest, isCyclic) {
    // topology1
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp3->addChild(filterOp6);
    EXPECT_TRUE(filterOp6->isCyclic());
    EXPECT_TRUE(filterOp3->isCyclic());
    EXPECT_FALSE(filterOp1->isCyclic());
    EXPECT_FALSE(filterOp2->isCyclic());
    EXPECT_FALSE(filterOp4->isCyclic());
}
/**
 *                                  /-> filterOp4 --|
 * topology1: filterOp6 -> filterOp1 -> filterOp2 <-
 *                    \-> filterOp3
 */
TEST_F(LogicalOperatorTest, isNotCyclic) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp4->addChild(filterOp2);

    EXPECT_FALSE(filterOp6->isCyclic());
    EXPECT_FALSE(filterOp3->isCyclic());
    EXPECT_FALSE(filterOp1->isCyclic());
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorTest, getAndFlattenAllSuccessorsNoCycle) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    std::vector<NodePtr> expected{};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllChildren(true);
    EXPECT_EQ(children.size(), expected.size());

    for (uint64_t i = 0; i < children.size(); i++) {
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

/**
 *                                  /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *            ^        \-> filterOp3
 *            |<---------------|
 */
TEST_F(LogicalOperatorTest, getAndFlattenAllSuccessorsForCycle) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);
    filterOp3->addChild(filterOp6);

    std::vector<NodePtr> expected{};
    expected.push_back(filterOp3);
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp4);

    children = filterOp6->getAndFlattenAllChildren(false);
    EXPECT_EQ(children.size(), expected.size());
    for (uint64_t i = 0; i < children.size(); i++) {
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

TEST_F(LogicalOperatorTest, prettyPrint) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    std::stringstream ss1;
    ss1 << std::string(0, ' ') << filterOp6->toString() << std::endl;
    ss1 << "|--" << filterOp3->toString() << std::endl;
    ss1 << "|--" << filterOp1->toString() << std::endl;
    ss1 << "|  |--" << filterOp2->toString() << std::endl;
    ss1 << "|  |--" << filterOp4->toString() << std::endl;

    std::stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    EXPECT_EQ(ss.str(), ss1.str());
}

TEST_F(LogicalOperatorTest, instanceOf) {
    bool inst = false;
    inst = filterOp1->instanceOf<LogicalFilterOperator>();
    EXPECT_TRUE(inst);
    inst = filterOp1->instanceOf<SourceLogicalOperator>();
    EXPECT_FALSE(inst);
}

TEST_F(LogicalOperatorTest, getOperatorByType) {
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    filterOp3->addChild(filterOp4);
    filterOp4->addChild(filterOp4);
    dumpContext->dump(filterOp6);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);
    expected.push_back(filterOp3);
    expected.push_back(filterOp4);
    const vector<LogicalFilterOperatorPtr> children = filterOp1->getNodesByType<LogicalFilterOperator>();

    // EXPECT_EQ(children.size(), expected.size());

    for (uint64_t i = 0; i < children.size(); i++) {
        NES_DEBUG("{}", i);
        // both reference to the same pointer
        EXPECT_TRUE(children[i]->isIdentical(expected[i]));
        // EXPECT_TRUE(children[i] == (expected[i]));
        // both have same values
        EXPECT_TRUE(children[i]->equal(expected[i]));
    }
}

/**
 *  swap filterOp1 by filterOp3
 *                                 /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorTest, swap1) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp1);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << "|--" << filterOp3->toString() << std::endl;
    expected << "|  |--" << filterOp2->toString() << std::endl;
    expected << "|  |--" << filterOp5->toString() << std::endl;

    stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    // std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp4 by filterOp3
 *                                 /-> filterOp4
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorTest, swap2) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp4);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp4);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << "|--" << filterOp1->toString() << std::endl;
    expected << "|  |--" << filterOp2->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp2->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    // std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp2 by filterOp3
 *                    /-> filterOp4 ->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp7
 */
TEST_F(LogicalOperatorTest, swap3) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp7);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << "|--" << filterOp1->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp7->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    expected << "|--" << filterOp4->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp7->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp2 by filterOp3
 *                    /-> filterOp4 ->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp7
 */
TEST_F(LogicalOperatorTest, swap4) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp7);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << "|--" << filterOp1->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp7->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    expected << "|--" << filterOp4->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp7->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp2 by filterOp3
 *                        /-> filterOp4 -->|
 * topology1: filterOp6 -> filterOp1 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorTest, swap5) {
    filterOp6->addChild(filterOp1);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    filterOp6->swap(filterOp3, filterOp2);
    stringstream expected;
    expected << std::string(0, ' ') << filterOp6->toString() << std::endl;
    expected << "|--" << filterOp1->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp2->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    expected << "|--" << filterOp4->toString() << std::endl;
    expected << "|  |--" << filterOp3->toString() << std::endl;
    expected << "|  |  |--" << filterOp2->toString() << std::endl;
    expected << "|  |  |--" << filterOp5->toString() << std::endl;

    stringstream ss;
    ConsoleDumpHandler::create(ss)->dump(filterOp6);
    std::cout << ss.str();
    EXPECT_EQ(ss.str(), expected.str());
}

/**
 *  swap filterOp4 by filterOp3
 *                    /-> filterOp4 -->|
 * topology1: filterOp6 -> filterOp3 -> filterOp2
 *
 *             filterOp3 -> filterOp5
 *                     \-> filterOp2
 */
TEST_F(LogicalOperatorTest, swap6) {
    filterOp6->addChild(filterOp3);
    filterOp6->addChild(filterOp4);
    filterOp1->addChild(filterOp2);
    filterOp4->addChild(filterOp2);

    filterOp3->addChild(filterOp2);
    filterOp3->addChild(filterOp5);

    bool swapped = filterOp6->swap(filterOp3, filterOp4);
    EXPECT_FALSE(swapped);
}

/**
 * split at filterOp2
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorTest, splitWithSinglePredecessor) {
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp1);
    expected.push_back(filterOp2);

    auto vec = filterOp6->split(filterOp2);
    EXPECT_EQ(vec.size(), expected.size());
    for (uint64_t i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}

/**
 * split at filterOp3
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorTest, splitWithAtLastSuccessor) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp2);
    expected.push_back(filterOp3);

    auto vec = filterOp6->split(filterOp3);
    EXPECT_EQ(vec.size(), expected.size());
    for (uint64_t i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}
/**
 * split at filterOp6
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorTest, splitWithAtRoot) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp6);

    auto vec = filterOp6->split(filterOp6);
    EXPECT_EQ(vec.size(), expected.size());
    for (uint64_t i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}
/**
 *
 *             filterOp7->\
 * topology1: filterOp6 -> filterOp1 -> filterOp2 -> filterOp3
 */
TEST_F(LogicalOperatorTest, splitWithMultiplePredecessors) {
    filterOp7->addChild(filterOp1);
    filterOp6->addChild(filterOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(filterOp3);
    std::vector<NodePtr> expected{};
    expected.push_back(filterOp7);
    expected.push_back(filterOp6);
    expected.push_back(filterOp1);

    auto vec = filterOp6->split(filterOp1);
    EXPECT_EQ(vec.size(), expected.size());
    for (uint64_t i = 0; i < vec.size(); i++) {
        EXPECT_TRUE(vec[i]->equal(expected[i]));
    }
}

TEST_F(LogicalOperatorTest, bfIterator) {
    /**
     * 1 -> 2 -> 5
     * 1 -> 2 -> 6
     * 1 -> 3
     * 1 -> 4 -> 7
     *
     */
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);
    filterOp1->addChild(filterOp4);
    filterOp2->addChild(filterOp5);
    filterOp2->addChild(filterOp6);
    filterOp4->addChild(filterOp7);

    dumpContext->dump(filterOp1);

    auto bfNodeIterator = BreadthFirstNodeIterator(filterOp1);
    auto iterator = bfNodeIterator.begin();

    ASSERT_EQ(*iterator, filterOp1);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp2);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp3);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp4);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp5);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp6);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp7);
}

TEST_F(LogicalOperatorTest, dfIterator) {
    /**
     * 1 -> 2 -> 5
     * 1 -> 2 -> 6
     * 1 -> 3
     * 1 -> 4 -> 7
     *
     */
    filterOp1->addChild(filterOp2);
    filterOp1->addChild(filterOp3);
    filterOp1->addChild(filterOp4);
    filterOp2->addChild(filterOp5);
    filterOp2->addChild(filterOp6);
    filterOp4->addChild(filterOp7);

    dumpContext->dump(filterOp1);

    auto dfNodeIterator = DepthFirstNodeIterator(filterOp1);
    auto iterator = dfNodeIterator.begin();

    ASSERT_EQ(*iterator, filterOp1);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp4);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp7);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp3);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp2);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp6);
    ++iterator;
    ASSERT_EQ(*iterator, filterOp5);
}

}// namespace NES
