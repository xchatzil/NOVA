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

#include <BaseUnitTest.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>

namespace NES::Runtime::Execution {
using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class CountMinBuildExecutionTest
    : public Testing::BaseUnitTest,
      public ::testing::WithParamInterface<std::tuple<uint64_t, uint64_t, uint64_t, Statistic::StatisticDataCodec>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr uint64_t defaultDecomposedQueryPlanId = 0;
    static constexpr uint64_t defaultSharedQueryId = 0;
    static constexpr uint64_t numberOfBitsInKey = sizeof(uint64_t) * 8;
    uint64_t sketchWidth, sketchDepth;
    SchemaPtr inputSchema, outputSchema;
    std::string fieldToBuildCountMinOver = "f1";
    std::string timestampFieldName = "ts";
    Statistic::StatisticFormatPtr statisticFormat;
    Statistic::StatisticMetricHash metricHash;
    Statistic::StatisticStorePtr testStatisticStore;
    Statistic::StatisticDataCodec statisticDataCodec;
    Statistic::SendingPolicyPtr sendingPolicy;
    Statistic::TriggerConditionPtr triggerCondition;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("CountMinBuildExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup CountMinBuildExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup CountMinBuildExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        const auto curTestCaseParams = CountMinBuildExecutionTest::GetParam();
        const auto numWorkerThreads = std::get<0>(CountMinBuildExecutionTest::GetParam());
        sketchWidth = std::get<1>(CountMinBuildExecutionTest::GetParam());
        sketchDepth = std::get<2>(CountMinBuildExecutionTest::GetParam());
        statisticDataCodec = std::get<3>(CountMinBuildExecutionTest::GetParam());

        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode, numWorkerThreads);

        inputSchema = Schema::create()
                          ->addField(fieldToBuildCountMinOver, BasicType::INT64)
                          ->addField(timestampFieldName, BasicType::UINT64)
                          ->updateSourceName("test");
        fieldToBuildCountMinOver =
            inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + fieldToBuildCountMinOver;
        timestampFieldName = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + timestampFieldName;

        outputSchema = Schema::create()
                           ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                           ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::DEPTH_FIELD_NAME, BasicType::UINT64)
                           ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, DataTypeFactory::createText())
                           ->updateSourceName("test");
        testStatisticStore = Statistic::DefaultStatisticStore::create();
        statisticFormat =
            Statistic::StatisticFormatFactory::createFromSchema(outputSchema,
                                                                executionEngine->getBufferManager()->getBufferSize(),
                                                                Statistic::StatisticSynopsisType::COUNT_MIN,
                                                                statisticDataCodec);
        metricHash = 42;// Just some arbitrary number
        sendingPolicy = Statistic::SendingPolicyASAP::create(statisticDataCodec);
        triggerCondition = Statistic::NeverTrigger::create();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down CountMinBuildExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs the statistic query and checks if the created count min sketches are correct by creating
     * count min sketches on our own and then comparing expected versus actual
     * @param testSourceDescriptor
     * @param testSinkDescriptor
     * @param countMinDescriptor
     * @param windowSize
     * @param windowSlide
     * @param allInputBuffers
     */
    void runQueryAndCheckCorrectness(SourceDescriptorPtr testSourceDescriptor,
                                     SinkDescriptorPtr testSinkDescriptor,
                                     Statistic::WindowStatisticDescriptorPtr countMinDescriptor,
                                     uint64_t windowSize,
                                     uint64_t windowSlide,
                                     TimeCharacteristicPtr timeCharacteristic,
                                     std::vector<TupleBuffer> allInputBuffers) {

        // Creating the query
        auto window = SlidingWindow::of(timeCharacteristic, Milliseconds(windowSize), Milliseconds(windowSlide));
        auto query = TestQuery::from(testSourceDescriptor)
                         .buildStatistic(window, countMinDescriptor, metricHash, sendingPolicy, triggerCondition)
                         .sink(testSinkDescriptor);

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(DecomposedQueryId(defaultDecomposedQueryPlanId),
                                                               SharedQueryId(defaultSharedQueryId),
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers and creating the expected count min sketches in testStatisticStore
        auto source = executionEngine->getDataSource(plan, 0);
        for (auto buf : allInputBuffers) {
            // We call here emit buffer without changing the metadata of the buffer, due to as setting it
            // in Util::createDataForOneFieldAndTimeStamp()
            source->emitBuffer(buf, false);

            // Now creating the expected count min sketches in testStatisticStore
            auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buf, inputSchema);
            Util::updateTestCountMinStatistic(dynamicBuffer,
                                              testStatisticStore,
                                              metricHash,
                                              numberOfBitsInKey,
                                              windowSize,
                                              windowSlide,
                                              sketchWidth,
                                              sketchDepth,
                                              fieldToBuildCountMinOver,
                                              timestampFieldName);
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(plan, Runtime::QueryTerminationType::Graceful));
        const auto nodeEngine = executionEngine->getNodeEngine();
        const auto nodeEngineStatisticStore = nodeEngine->getStatisticManager()->getStatisticStore();

        while (nodeEngineStatisticStore->getAllStatistics().size() != testStatisticStore->getAllStatistics().size()) {
            NES_DEBUG("Waiting till all statistics have been built. Currently {} and expecting {}...",
                      nodeEngineStatisticStore->getAllStatistics().size(),
                      testStatisticStore->getAllStatistics().size());
            std::this_thread::sleep_for(100ms);
        }

        // Checking the correctness
        NES_DEBUG("Built all statistics. Now checking for correctness...");
        for (auto& [statisticHash, countMinStatistic] : nodeEngineStatisticStore->getAllStatistics()) {
            NES_DEBUG("Searching for statisticHash = {} countMinStatistic = {}", statisticHash, countMinStatistic->toString());
            auto expectedStatistics =
                testStatisticStore->getStatistics(statisticHash, countMinStatistic->getStartTs(), countMinStatistic->getEndTs());
            EXPECT_EQ(expectedStatistics.size(), 1);
            NES_DEBUG("Found for statisticHash = {} expectedStatistics = {}", statisticHash, expectedStatistics[0]->toString());
            EXPECT_TRUE(expectedStatistics[0]->equal(*countMinStatistic));
        }
    }
};

/**
 * @brief Here we test, if we create a count min sketch for a single input tuple
 */
TEST_P(CountMinBuildExecutionTest, singleInputTuple) {
    using namespace Statistic;
    constexpr auto windowSize = 10;
    constexpr auto numberOfTuples = 1;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildCountMinOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::COUNT_MIN, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the count min descriptor and running the query
    auto countMinDescriptor = CountMinDescriptor::create(Over(fieldToBuildCountMinOver), sketchWidth, sketchDepth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                countMinDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple count min sketches for multiple input buffers, but also for larger sketches
 */
TEST_P(CountMinBuildExecutionTest, multipleInputBuffers) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildCountMinOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::COUNT_MIN, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the count min descriptor and running the query
    auto countMinDescriptor = CountMinDescriptor::create(Over(fieldToBuildCountMinOver), sketchWidth, sketchDepth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                countMinDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple count min sketches for multiple input buffers, but also for larger sketches
 * and for sliding windows (we create one sketch per slice)
 */
TEST_P(CountMinBuildExecutionTest, multipleInputBuffersSlidingWindow) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto windowSlide = 500;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = EventTime(Attribute(timestampFieldName));
    const auto isIngestionTime = false;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildCountMinOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::COUNT_MIN, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the count min descriptor and running the query
    auto countMinDescriptor = CountMinDescriptor::create(Over(fieldToBuildCountMinOver), sketchWidth, sketchDepth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                countMinDescriptor,
                                windowSize,
                                windowSlide,
                                timeCharacteristic,
                                allInputBuffers);
}

/**
 * @brief Here we test, if we create multiple count min sketches for multiple input buffers, but also for larger sketches.
 * The difference is that we use the ingestion time instead of an event time
 */
TEST_P(CountMinBuildExecutionTest, multipleInputBuffersIngestionTime) {
    using namespace Statistic;
    constexpr auto windowSize = 1000;
    constexpr auto numberOfTuples = 1'000;
    const auto timeCharacteristic = IngestionTime();
    const auto isIngestionTime = true;
    auto allInputBuffers = Util::createDataForOneFieldAndTimeStamp(numberOfTuples,
                                                                   *executionEngine->getBufferManager(),
                                                                   inputSchema,
                                                                   fieldToBuildCountMinOver,
                                                                   timestampFieldName,
                                                                   isIngestionTime);

    // Creating the sink and the sources
    const auto testSinkDescriptor = StatisticSinkDescriptor::create(StatisticSynopsisType::COUNT_MIN, statisticDataCodec);
    const auto testSourceDescriptor = executionEngine->createDataSource(inputSchema);

    // Creating the count min descriptor and running the query
    auto countMinDescriptor = CountMinDescriptor::create(Over(fieldToBuildCountMinOver), sketchWidth, sketchDepth);
    runQueryAndCheckCorrectness(testSourceDescriptor,
                                testSinkDescriptor,
                                countMinDescriptor,
                                windowSize,
                                windowSize,
                                timeCharacteristic,
                                allInputBuffers);
}

INSTANTIATE_TEST_CASE_P(testCountMin,
                        CountMinBuildExecutionTest,
                        ::testing::Combine(::testing::Values(1, 4, 8),     // numWorkerThread
                                           ::testing::Values(1, 100, 4000),// sketchWidth
                                           ::testing::Values(1, 5, 10),    // sketchDepth
                                           ::testing::ValuesIn(            // All possible statistic sink datatype
                                               magic_enum::enum_values<Statistic::StatisticDataCodec>())),
                        [](const testing::TestParamInfo<CountMinBuildExecutionTest::ParamType>& info) {
                            const auto numWorkerThread = std::get<0>(info.param);
                            const auto sketchWidth = std::get<1>(info.param);
                            const auto sketchDepth = std::get<2>(info.param);
                            const auto dataCodec = std::get<3>(info.param);
                            return std::to_string(numWorkerThread) + "Threads_" + std::to_string(sketchWidth) + "Width_"
                                + std::to_string(sketchDepth) + "Depth_" + std::string(magic_enum::enum_name(dataCodec));
                        });
}// namespace NES::Runtime::Execution
