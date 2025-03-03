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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <string>

namespace NES::Runtime::Execution {

class NestedLoopJoinMockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    NestedLoopJoinMockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                                 uint64_t noWorkerThreads,
                                                 OperatorHandlerPtr nljOpHandler,
                                                 PipelineId pipelineId)
        : PipelineExecutionContext(
            pipelineId,          // mock pipeline id
            DecomposedQueryId(1),// mock query id
            bufferManager,
            noWorkerThreads,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            {nljOpHandler}){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class NestedLoopJoinPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {

  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;
    Nautilus::CompilationOptions options;
    const uint64_t leftPageSize = 256;
    const uint64_t rightPageSize = 512;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup NestedLoopJoinPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
    }

    bool checkIfNLJWorks(const std::string& fileNameBuffersLeft,
                         const std::string& fileNameBuffersRight,
                         const std::string& fileNameBuffersSink,
                         uint64_t windowSize,
                         const SchemaPtr leftSchema,
                         const SchemaPtr rightSchema,
                         const SchemaPtr joinSchema,
                         const std::string& timeStampFieldLeft,
                         const std::string& timeStampFieldRight,
                         const std::string& windowStartFieldName,
                         const std::string& windowEndFieldName,
                         Expressions::ExpressionPtr joinExpression) {

        bool nljWorks = true;

        // Creating the input left and right buffers and the expected output buffer
        auto originId = 0UL;
        auto leftBuffers =
            Util::createBuffersFromCSVFile(fileNameBuffersLeft, leftSchema, bufferManager, originId++, timeStampFieldLeft);
        auto rightBuffers =
            Util::createBuffersFromCSVFile(fileNameBuffersRight, rightSchema, bufferManager, originId++, timeStampFieldRight);
        auto expectedSinkBuffers = Util::createBuffersFromCSVFile(fileNameBuffersSink, joinSchema, bufferManager, originId++);
        NES_DEBUG("read file={}", fileNameBuffersSink);

        NES_DEBUG("leftBuffer: \n{}", Util::printTupleBufferAsCSV(leftBuffers[0], leftSchema));
        NES_DEBUG("rightBuffers: \n{}", Util::printTupleBufferAsCSV(rightBuffers[0], rightSchema));

        // Creating the scan (for build) and emit operator (for sink)
        auto memoryLayoutLeft = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bufferManager->getBufferSize());
        auto memoryLayoutRight = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bufferManager->getBufferSize());
        auto memoryLayoutJoined = Runtime::MemoryLayouts::RowLayout::create(joinSchema, bufferManager->getBufferSize());

        auto scanMemoryProviderLeft = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutLeft);
        auto scanMemoryProviderRight = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutRight);
        auto emitMemoryProviderSink = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutJoined);

        auto scanOperatorLeft = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderLeft));
        auto scanOperatorRight = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderRight));
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderSink));

        // Creating the left, right and sink NLJ operator
        const auto handlerIndex = 0;
        const auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timeStampFieldLeft);
        const auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(timeStampFieldRight);
        const auto leftEntrySize = leftSchema->getSchemaSizeInBytes();
        const auto rightEntrySize = rightSchema->getSchemaSizeInBytes();

        auto nljBuildLeft = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            leftSchema,
            QueryCompilation::JoinBuildSideType::Left,
            leftEntrySize,
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);

        auto nljBuildRight = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            rightSchema,
            QueryCompilation::JoinBuildSideType::Right,
            rightEntrySize,
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);

        Operators::JoinSchema joinSchemaStruct(leftSchema, rightSchema, Util::createJoinSchema(leftSchema, rightSchema));
        Operators::WindowMetaData windowMetaData(windowStartFieldName, windowEndFieldName);

        auto nljProbe = std::make_shared<Operators::NLJProbe>(handlerIndex,
                                                              joinSchemaStruct,
                                                              joinExpression,
                                                              windowMetaData,
                                                              leftSchema,
                                                              rightSchema,
                                                              QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                                              QueryCompilation::WindowingStrategy::SLICING);

        // Creating the NLJ operator handler
        std::vector<OriginId> originIds{INVALID_ORIGIN_ID, OriginId(1)};
        OriginId outputOriginId = OriginId(1);
        auto nljOperatorHandler = Operators::NLJOperatorHandlerSlicing::create(originIds,
                                                                               outputOriginId,
                                                                               windowSize,
                                                                               windowSize,
                                                                               leftSchema,
                                                                               rightSchema,
                                                                               leftPageSize,
                                                                               rightPageSize);

        // Building the pipeline
        auto pipelineBuildLeft = std::make_shared<PhysicalOperatorPipeline>();
        auto pipelineBuildRight = std::make_shared<PhysicalOperatorPipeline>();
        auto pipelineSink = std::make_shared<PhysicalOperatorPipeline>();

        scanOperatorLeft->setChild(nljBuildLeft);
        scanOperatorRight->setChild(nljBuildRight);
        nljProbe->setChild(emitOperator);

        pipelineBuildLeft->setRootOperator(scanOperatorLeft);
        pipelineBuildRight->setRootOperator(scanOperatorRight);
        pipelineSink->setRootOperator(nljProbe);

        auto curPipelineId = 0;
        auto noWorkerThreads = 1;
        auto pipelineExecCtxLeft = NestedLoopJoinMockedPipelineExecutionContext(bufferManager,
                                                                                noWorkerThreads,
                                                                                nljOperatorHandler,
                                                                                PipelineId(curPipelineId++));
        auto pipelineExecCtxRight = NestedLoopJoinMockedPipelineExecutionContext(bufferManager,
                                                                                 noWorkerThreads,
                                                                                 nljOperatorHandler,
                                                                                 PipelineId(curPipelineId++));
        auto pipelineExecCtxSink = NestedLoopJoinMockedPipelineExecutionContext(bufferManager,
                                                                                noWorkerThreads,
                                                                                nljOperatorHandler,
                                                                                PipelineId(curPipelineId++));

        nljOperatorHandler->start(std::make_shared<PipelineExecutionContext>(pipelineExecCtxLeft), 0);

        auto executablePipelineLeft = provider->create(pipelineBuildLeft, options);
        auto executablePipelineRight = provider->create(pipelineBuildRight, options);
        auto executablePipelineSink = provider->create(pipelineSink, options);

        nljWorks = (executablePipelineLeft->setup(pipelineExecCtxLeft) == 0);
        nljWorks = nljWorks && (executablePipelineRight->setup(pipelineExecCtxRight) == 0);
        nljWorks = nljWorks && (executablePipelineSink->setup(pipelineExecCtxSink) == 0);

        // Executing left and right buffers
        for (auto buffer : leftBuffers) {
            executablePipelineLeft->execute(buffer, pipelineExecCtxLeft, *workerContext);
        }
        for (auto buffer : rightBuffers) {
            executablePipelineRight->execute(buffer, pipelineExecCtxRight, *workerContext);
        }
        nljWorks = nljWorks && (executablePipelineLeft->stop(pipelineExecCtxLeft) == 0);
        nljWorks = nljWorks && (executablePipelineRight->stop(pipelineExecCtxRight) == 0);
        nljOperatorHandler->stop(QueryTerminationType::Graceful, std::make_shared<PipelineExecutionContext>(pipelineExecCtxLeft));
        nljOperatorHandler->stop(QueryTerminationType::Graceful,
                                 std::make_shared<PipelineExecutionContext>(pipelineExecCtxRight));

        // Assure that at least one buffer has been emitted
        nljWorks = nljWorks && (!pipelineExecCtxLeft.emittedBuffers.empty() || !pipelineExecCtxRight.emittedBuffers.empty());

        // Executing sink buffers
        std::vector<Runtime::TupleBuffer> buildEmittedBuffers(pipelineExecCtxLeft.emittedBuffers);
        buildEmittedBuffers.insert(buildEmittedBuffers.end(),
                                   pipelineExecCtxRight.emittedBuffers.begin(),
                                   pipelineExecCtxRight.emittedBuffers.end());
        for (auto buf : buildEmittedBuffers) {
            executablePipelineSink->execute(buf, pipelineExecCtxSink, *workerContext);
        }
        nljWorks = nljWorks && (executablePipelineSink->stop(pipelineExecCtxSink) == 0);
        std::vector<SequenceData> seqNumbers;

        std::transform(pipelineExecCtxSink.emittedBuffers.begin(),
                       pipelineExecCtxSink.emittedBuffers.end(),
                       std::back_inserter(seqNumbers),
                       [](const TupleBuffer& buffer) {
                           return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk());
                       });
        std::sort(seqNumbers.begin(), seqNumbers.end());
        bool hasDuplicates = std::adjacent_find(seqNumbers.begin(), seqNumbers.end()) != seqNumbers.end();
        nljWorks &= !hasDuplicates;
        if (hasDuplicates) {
            NES_ERROR("Result contains multiple buffer with the same Sequence Number");
        }
        auto resultBuffer = Util::mergeBuffers(pipelineExecCtxSink.emittedBuffers, joinSchema, bufferManager);
        NES_DEBUG("resultBuffer: \n{}", Util::printTupleBufferAsCSV(resultBuffer, joinSchema));
        NES_DEBUG("expectedSinkBuffer: \n{}", Util::printTupleBufferAsCSV(expectedSinkBuffers[0], joinSchema));

        nljWorks = nljWorks && (resultBuffer.getNumberOfTuples() == expectedSinkBuffers[0].getNumberOfTuples());
        nljWorks =
            nljWorks && (Util::checkIfBuffersAreEqual(resultBuffer, expectedSinkBuffers[0], joinSchema->getSchemaSizeInBytes()));
        return nljWorks;
    }
};

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipeline) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();

    auto onLeftKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameLeft);
    auto onRightKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameRight);
    auto joinExpression = std::make_shared<Expressions::EqualsExpression>(onLeftKey, onRightKey);

    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_sink.csv");

    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                joinExpression));
}

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineDifferentInput) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();

    auto onLeftKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameLeft);
    auto onRightKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameRight);
    auto joinExpression = std::make_shared<Expressions::EqualsExpression>(onLeftKey, onRightKey);

    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_sink2.csv");

    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                joinExpression));
}

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineMultipleKeyExpresssionsDifferentExpressions) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldName1Right = rightSchema->get(0)->getName();
    const auto joinFieldName1Left = leftSchema->get(0)->getName();
    const auto joinFieldName2Right = rightSchema->get(1)->getName();
    const auto joinFieldName2Left = leftSchema->get(1)->getName();

    //create joinExpression: id == id && value > value
    auto onLeftKey1 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName1Left);
    auto onRightKey1 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName1Right);
    auto onLeftKey2 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName2Left);
    auto onRightKey2 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName2Right);
    auto joinExpressionEq = std::make_shared<Expressions::EqualsExpression>(onLeftKey1, onRightKey1);
    auto joinExpressionGt = std::make_shared<Expressions::GreaterThanExpression>(onLeftKey2, onRightKey2);
    auto joinExpression = std::make_shared<Expressions::AndExpression>(joinExpressionEq, joinExpressionGt);

    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_sink4.csv");

    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                joinExpression));
}

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineCrossJoinTest) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    auto joinExpression = std::make_shared<Expressions::ConstantBooleanValueExpression>(true);

    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_cross_join_sink.csv");

    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                joinExpression));
}

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineMultipleKeyExpresssionsDifferentExpressionsNoEquals) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$value", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldName1Right = rightSchema->get(0)->getName();
    const auto joinFieldName1Left = leftSchema->get(0)->getName();
    const auto joinFieldName2Right = rightSchema->get(1)->getName();
    const auto joinFieldName2Left = leftSchema->get(1)->getName();

    //create joinExpression: id <= id && value > value
    auto onLeftKey1 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName1Left);
    auto onRightKey1 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName1Right);
    auto onLeftKey2 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName2Left);
    auto onRightKey2 = std::make_shared<Expressions::ReadFieldExpression>(joinFieldName2Right);
    auto keyExpressionEq = std::make_shared<Expressions::LessEqualsExpression>(onLeftKey1, onRightKey1);
    auto keyExpressionGt = std::make_shared<Expressions::GreaterThanExpression>(onLeftKey2, onRightKey2);
    auto keyExpressions = std::make_shared<Expressions::AndExpression>(keyExpressionEq, keyExpressionGt);

    const auto timeStampFieldRight = rightSchema->get(2)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_sink5.csv");

    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                keyExpressions));
}

TEST_P(NestedLoopJoinPipelineTest, nljSimplePipelineDifferentNumberOfAttributes) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$id", BasicType::UINT64)
                                ->addField("left$value", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$id", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = rightSchema->get(0)->getName();
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();

    auto onLeftKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameLeft);
    auto onRightKey = std::make_shared<Expressions::ReadFieldExpression>(joinFieldNameRight);
    auto keyExpressions = std::make_shared<Expressions::EqualsExpression>(onLeftKey, onRightKey);

    const auto timeStampFieldRight = rightSchema->get(1)->getName();
    const auto timeStampFieldLeft = leftSchema->get(2)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();

    // read values from csv file into one buffer for each join side and for one window
    const auto windowSize = 1000UL;
    const std::string fileNameBuffersLeft(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    const std::string fileNameBuffersRight(std::filesystem::path(TEST_DATA_DIRECTORY) / "window3.csv");
    const std::string fileNameBuffersSink(std::filesystem::path(TEST_DATA_DIRECTORY) / "window_sink3.csv");
    ASSERT_TRUE(checkIfNLJWorks(fileNameBuffersLeft,
                                fileNameBuffersRight,
                                fileNameBuffersSink,
                                windowSize,
                                leftSchema,
                                rightSchema,
                                joinSchema,
                                timeStampFieldLeft,
                                timeStampFieldRight,
                                windowStartFieldName,
                                windowEndFieldName,
                                keyExpressions));
}

INSTANTIATE_TEST_CASE_P(nestedLoopJoinPipelineTest,
                        NestedLoopJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<NestedLoopJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution
