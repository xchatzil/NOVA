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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <gmock/gmock-matchers.h>

namespace NES::Runtime::Execution {

using namespace std::chrono_literals;
constexpr auto queryCompilerDumpMode = NES::QueryCompilation::DumpMode::NONE;

class StreamJoinQueryExecutionTest : public Testing::BaseUnitTest,
                                     public ::testing::WithParamInterface<
                                         std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy>> {
  public:
    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;
    static constexpr DecomposedQueryId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
    static constexpr std::chrono::milliseconds defaultTimeout = 1s;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
        Testing::BaseUnitTest::SetUp();
        joinStrategy = std::get<0>(NES::Runtime::Execution::StreamJoinQueryExecutionTest::GetParam());
        windowingStrategy = std::get<1>(NES::Runtime::Execution::StreamJoinQueryExecutionTest::GetParam());
        const auto numWorkerThreads = 1;
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompilerDumpMode,
                                                                         numWorkerThreads,
                                                                         joinStrategy,
                                                                         windowingStrategy);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        EXPECT_TRUE(executionEngine->stop());
        Testing::BaseUnitTest::TearDown();
    }

    /**
     * @brief Runs a join query with the inputs (Schema, CSVFile). Currently, we only support csv files as the input.
     * This test does not temrminate the query, which is important if the test should examine the join results without forcefully
     * flushing the joins windows.
     * @tparam ResultRecord
     * @param inputs
     * @param expectedNumberOfTuples
     * @param testSink
     * @param query
     * @return (Vector of ResultRecords, QueryPlan which needs to be passed to stopquery)
     */
    template<typename ResultRecord>
    std::pair<std::vector<ResultRecord>, ExecutableQueryPlanPtr>
    runQueryWithCsvFilesWithoutStoppingQuery(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                             const uint64_t expectedNumberOfTuples,
                                             const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                             const Query& query) {

        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (const auto& [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::createExpectedBuffersFromCsv(fileNameInputBuffers, inputSchema, bufferManager));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                               defaultSharedQueryId,
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers
        auto dataSourceCount = 0_u64;
        for (auto& inputBuffers : allInputBuffers) {
            const auto source = executionEngine->getDataSource(plan, dataSourceCount);
            dataSourceCount++;
            for (auto& buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // A correct implementation should produce the expectedNumberOfTuples within the defaultTimeout.
        testSink->waitTillCompletedOrTimeout(expectedNumberOfTuples, defaultTimeout.count());
        // Checking for correctness
        return {testSink->getResult(), plan};
    }
    /**
     * @brief Runs a join query with the inputs (Schema, CSVFile). Currently, we only support csv files as the input
     * @tparam ResultRecord
     * @param inputs
     * @param expectedNumberOfTuples
     * @param testSink
     * @param query
     * @return Vector of ResultRecords
     */
    template<typename ResultRecord>
    std::vector<ResultRecord> runQueryWithCsvFiles(const std::vector<std::pair<SchemaPtr, std::string>>& inputs,
                                                   const uint64_t expectedNumberOfTuples,
                                                   const std::shared_ptr<CollectTestSink<ResultRecord>>& testSink,
                                                   const Query& query) {

        // Creating the input buffers
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<std::vector<Runtime::TupleBuffer>> allInputBuffers;
        allInputBuffers.reserve(inputs.size());
        for (auto [inputSchema, fileNameInputBuffers] : inputs) {
            allInputBuffers.emplace_back(
                TestUtils::createExpectedBuffersFromCsv(fileNameInputBuffers, inputSchema, bufferManager));
        }

        // Creating query and submitting it to the execution engine
        NES_INFO("Submitting query: {}", query.getQueryPlan()->toString())
        auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                               defaultSharedQueryId,
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto plan = executionEngine->submitQuery(decomposedQueryPlan);

        // Emitting the input buffers
        auto dataSourceCount = 0_u64;
        for (const auto& inputBuffers : allInputBuffers) {
            auto source = executionEngine->getDataSource(plan, dataSourceCount);
            dataSourceCount++;
            for (auto buf : inputBuffers) {
                source->emitBuffer(buf);
            }
        }

        // Giving the execution engine time to process the tuples, so that we do not just test our terminate() implementation
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Stopping query and waiting until the test sink has received the expected number of tuples
        NES_INFO("Stopping query now!!!");
        EXPECT_TRUE(executionEngine->stopQuery(plan, Runtime::QueryTerminationType::Graceful));
        testSink->waitTillCompleted(expectedNumberOfTuples);

        // Checking for correctness
        return testSink->getResult();
    }

    template<typename ResultRecord>
    std::vector<ResultRecord> runSingleJoinQuery(const TestUtils::CsvFileParams& csvFileParams,
                                                 const TestUtils::JoinParams& joinParams,
                                                 const WindowTypePtr& joinWindow,
                                                 const std::string keyLeft,
                                                 const std::string keyRight) {
        // Getting the expected output tuples
        auto bufferManager = executionEngine->getBufferManager();

        std::vector<ResultRecord> expectedSinkVector;
        const auto expectedSinkBuffers =
            TestUtils::createExpectedBuffersFromCsv(csvFileParams.expectedFile, joinParams.outputSchema, bufferManager);

        for (const auto& buf : expectedSinkBuffers) {
            const auto tmpVec = TestUtils::createVecFromTupleBuffer<ResultRecord>(buf);
            expectedSinkVector.insert(expectedSinkVector.end(), tmpVec.begin(), tmpVec.end());
        }

        const auto testSink = executionEngine->createCollectSink<ResultRecord>(joinParams.outputSchema);
        const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
        const auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
        const auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

        auto query = TestQuery::from(testSourceDescriptorLeft)
                         .joinWith(TestQuery::from(testSourceDescriptorRight))
                         .where(Attribute(keyLeft) == Attribute(keyRight))
                         .window(joinWindow)
                         .sink(testSinkDescriptor);

        // Running the query
        auto resultRecords = runQueryWithCsvFiles<ResultRecord>({{joinParams.inputSchemas[0], csvFileParams.inputCsvFiles[0]},
                                                                 {joinParams.inputSchemas[1], csvFileParams.inputCsvFiles[1]}},
                                                                expectedSinkVector.size(),
                                                                testSink,
                                                                query);

        // Checking for correctness
        EXPECT_EQ(resultRecords.size(), expectedSinkVector.size());
        EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedSinkVector));

        // We return the result records, as someone might want to print them to std::out or do something else
        return resultRecords;
    }

    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
};

TEST_P(StreamJoinQueryExecutionTest, streamJoinExecutionTestCsvFiles) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t test1test2Start;
        uint64_t test1test2End;

        uint64_t test1f1_left;
        uint64_t test1f2_left;
        uint64_t test1timestamp;
        uint64_t test2f1_right;
        uint64_t test2f2_right;
        uint64_t test2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2Start == rhs.test1test2Start && test1test2End == rhs.test1test2End
                && test1f1_left == rhs.test1f1_left && test1f2_left == rhs.test1f2_left && test1timestamp == rhs.test1timestamp
                && test2f1_right == rhs.test2f1_right && test2f2_right == rhs.test2f2_right
                && test2timestamp == rhs.test2timestamp;
        }
    };

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(20);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("stream_join_left.csv", "stream_join_right.csv", "stream_join_sink.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    const auto resultRecords = runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "f2_left", "f2_right");
}

/**
* Test deploying join with same data and same schema
 * */
TEST_P(StreamJoinQueryExecutionTest, testJoinWithSameSchemaTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;

        uint64_t window1value;
        uint64_t window1id;
        uint64_t window1timestamp;

        uint64_t window2value;
        uint64_t window2id;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value == rhs.window1value && window1id == rhs.window1id && window1timestamp == rhs.window1timestamp
                && window2value == rhs.window2value && window2id == rhs.window2id && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id");
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_P(StreamJoinQueryExecutionTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2value2 == rhs.window2value2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window.csv", "window_sink.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

/**
 * Test deploying join with different sources
 */
TEST_P(StreamJoinQueryExecutionTest, testJoinWithDifferentSourceTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2value2 == rhs.window2value2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink2.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

/**
 * Test deploying join with different sources
 */
TEST_P(StreamJoinQueryExecutionTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2id2;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink3.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

/**
 * Test deploying join with different sources
 */
TEST_P(StreamJoinQueryExecutionTest, testJoinWithDifferentSourceSlidingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2value2 == rhs.window2value2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto windowSlide = Milliseconds(500);
    const auto timestampFieldName = "timestamp";
    const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize, windowSlide);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window2.csv", "window_sink5.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

TEST_P(StreamJoinQueryExecutionTest, testJoinWithLargerWindowSizes) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2value2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2value2 == rhs.window2value2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto windowSlide = Milliseconds(250);
    const auto timestampFieldName = "timestamp";
    const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize, windowSlide);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window7.csv", "window7.csv", "window_sink7.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

/**
 * Test deploying join with different sources
 */
TEST_P(StreamJoinQueryExecutionTest, testSlidingWindowDifferentAttributes) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;
        uint64_t window1value1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2id2;
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && window1value1 == rhs.window1value1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp;
        }
    };

    const auto leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName(*srcName);
    const auto rightSchema = TestSchemas::getSchemaTemplate("id2_time_u64")->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto windowSlide = Milliseconds(500);
    const auto timestampFieldName = "timestamp";
    const auto window = SlidingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize, windowSlide);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window.csv", "window3.csv", "window_sink6.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

/**
 * @brief Test a join query that uses fixed-array as keys
 */
// TODO this test can be enabled once #3638 is merged
TEST_P(StreamJoinQueryExecutionTest, DISABLED_testJoinWithFixedCharKey) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2Start;
        uint64_t window1window2End;

        char window1id1[7];
        uint64_t window1timestamp;
        char window2id2[7];
        uint64_t window2timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2Start == rhs.window1window2Start && window1window2End == rhs.window1window2End
                && std::memcmp(window1id1, rhs.window1id1, sizeof(char) * 7) == 0
                && std::memcmp(window2id2, rhs.window2id2, sizeof(char) * 7) == 0 && window1timestamp == rhs.window1timestamp
                && window2timestamp == rhs.window2timestamp;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("id1", DataTypeFactory::createText())
                                ->addField("timestamp", BasicType::UINT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("id2", DataTypeFactory::createText())
                                 ->addField("timestamp", BasicType::UINT64)
                                 ->updateSourceName(*srcName);

    const auto windowSize = Milliseconds(1000);
    const auto timestampFieldName = "timestamp";
    const auto window = TumblingWindow::of(EventTime(Attribute(timestampFieldName)), windowSize);

    // Running a single join query
    TestUtils::CsvFileParams csvFileParams("window5.csv", "window6.csv", "window_sink4.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});
    runSingleJoinQuery<ResultRecord>(csvFileParams, joinParams, window, "id", "id2");
}

TEST_P(StreamJoinQueryExecutionTest, streamJoinExecutiontTestWithSlidingWindows) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        int64_t test1test2$start;
        int64_t test1test2$end;

        int64_t test1$key;
        int64_t test1$values;
        int64_t test1$values2;
        int64_t test1$timestamp;

        int64_t test2$timestamp;
        int64_t test2$end;
        int64_t test2$key;
        uint64_t test2$count;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2$start == rhs.test1test2$start && test1test2$end == rhs.test1test2$end && test1$key == rhs.test1$key
                && test1$values == rhs.test1$values && test1$values2 == rhs.test1$values2
                && test1$timestamp == rhs.test1$timestamp && test2$timestamp == rhs.test2$timestamp && test2$end == rhs.test2$end
                && test2$key == rhs.test2$key && test2$count == rhs.test2$count;
        }
    };
    const auto leftSchema = TestSchemas::getSchemaTemplate("id_2val_time_u64")->updateSourceName(*srcName);

    const auto rightSchema = TestSchemas::getSchemaTemplate("id_2val_time_u64")->updateSourceName(*srcName);

    const auto sinkSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1test2$start", BasicType::INT64)
                                ->addField("test1test2$end", BasicType::INT64)

                                ->addField("test1$id", BasicType::INT64)
                                ->addField("test1$value", BasicType::INT64)
                                ->addField("test1$value2", BasicType::INT64)
                                ->addField("test1$timestamp", BasicType::INT64)

                                ->addField("test2$timestamp", BasicType::INT64)
                                ->addField("test2$end", BasicType::INT64)
                                ->addField("test2$id", BasicType::INT64)
                                ->addField("test2$count", BasicType::UINT64);

    TestUtils::CsvFileParams csvFileParams("stream_join_left_withCountSlidingWindow.csv",
                                           "stream_join_left_withCountSlidingWindow.csv",
                                           "stream_join_left_withCountSlidingWindow_result.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});

    // Getting the expected output tuples
    auto bufferManager = executionEngine->getBufferManager();
    const auto expectedSinkBuffer =
        TestUtils::createExpectedBuffersFromCsv(csvFileParams.expectedFile, sinkSchema, bufferManager)[0];
    const auto expectedSinkVector = TestUtils::createVecFromTupleBuffer<ResultRecord>(expectedSinkBuffer);

    // Creating the sink and the sources
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

    // Running the query
    auto query =
        TestQuery::from(testSourceDescriptorLeft)
            .joinWith(TestQuery::from(testSourceDescriptorRight)
                          .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                          .byKey(Attribute("id"))
                          .apply(Count())
                          .project(Attribute("start").as("timestamp"), Attribute("end"), Attribute("id"), Attribute("count")))
            .where(Attribute("id") == Attribute("id"))
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
            .sink(testSinkDescriptor);

    const auto [resultRecords, qp] =
        runQueryWithCsvFilesWithoutStoppingQuery<ResultRecord>({{joinParams.inputSchemas[0], csvFileParams.inputCsvFiles[0]},
                                                                {joinParams.inputSchemas[1], csvFileParams.inputCsvFiles[1]}},
                                                               expectedSinkVector.size(),
                                                               testSink,
                                                               query);

    NES_INFO("Manually stopping the Query");
    EXPECT_TRUE(executionEngine->stopQuery(qp, Runtime::QueryTerminationType::Graceful));

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedSinkVector.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedSinkVector));
}

TEST_P(StreamJoinQueryExecutionTest, streamJoinExecutiontTestWithWindows) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    struct __attribute__((packed)) ResultRecord {
        int64_t test1test2$start;
        int64_t test1test2$end;
        int64_t test1$start;
        int64_t test1$end;
        int64_t test1$f2_left;
        int64_t test1$fieldForSum1;
        int64_t test2$start;
        int64_t test2$end;
        int64_t test2$f2_right;
        int64_t test2$fieldForSum2;

        bool operator==(const ResultRecord& rhs) const {
            return test1test2$start == rhs.test1test2$start && test1test2$end == rhs.test1test2$end
                && test1$start == rhs.test1$start && test1$end == rhs.test1$end && test1$f2_left == rhs.test1$f2_left
                && test1$fieldForSum1 == rhs.test1$fieldForSum1 && test2$start == rhs.test2$start && test2$end == rhs.test2$end
                && test2$f2_right == rhs.test2$f2_right && test2$fieldForSum2 == rhs.test2$fieldForSum2;
        }
    };
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::INT64)
                                ->addField("f2_left", BasicType::INT64)
                                ->addField("timestamp", BasicType::INT64)
                                ->addField("fieldForSum1", BasicType::INT64)
                                ->updateSourceName(*srcName);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::INT64)
                                 ->addField("f2_right", BasicType::INT64)
                                 ->addField("timestamp", BasicType::INT64)
                                 ->addField("fieldForSum2", BasicType::INT64)
                                 ->updateSourceName(*srcName);

    const auto sinkSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("test1test2$start", BasicType::INT64)
                                ->addField("test1test2$end", BasicType::INT64)
                                ->addField("test1$start", BasicType::INT64)
                                ->addField("test1$end", BasicType::INT64)
                                ->addField("test1$f2_left", BasicType::INT64)
                                ->addField("test1$fieldForSum1", BasicType::INT64)
                                ->addField("test2$start", BasicType::INT64)
                                ->addField("test2$end", BasicType::INT64)
                                ->addField("test2$f2_right", BasicType::INT64)
                                ->addField("test2$fieldForSum2", BasicType::INT64);

    const auto timestampFieldName = "timestamp";
    const auto windowSize = 10UL;
    TestUtils::CsvFileParams csvFileParams("stream_join_left_withSum.csv",
                                           "stream_join_right_withSum.csv",
                                           "stream_join_withSum_sink.csv");
    TestUtils::JoinParams joinParams({leftSchema, rightSchema});

    // Getting the expected output tuples
    auto bufferManager = executionEngine->getBufferManager();
    const auto expectedSinkBuffer =
        TestUtils::createExpectedBuffersFromCsv(csvFileParams.expectedFile, sinkSchema, bufferManager)[0];
    const auto expectedSinkVector = TestUtils::createVecFromTupleBuffer<ResultRecord>(expectedSinkBuffer);

    // Creating the sink and the sources
    const auto testSink = executionEngine->createCollectSink<ResultRecord>(sinkSchema);
    const auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    const auto testSourceDescriptorLeft = executionEngine->createDataSource(joinParams.inputSchemas[0]);
    const auto testSourceDescriptorRight = executionEngine->createDataSource(joinParams.inputSchemas[1]);

    // Running the query
    auto query = TestQuery::from(testSourceDescriptorLeft)
                     .window(TumblingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(windowSize)))
                     .byKey(Attribute("f2_left"))
                     .apply(Sum(Attribute("fieldForSum1")))
                     .joinWith(TestQuery::from(testSourceDescriptorRight)
                                   .window(TumblingWindow::of(EventTime(Attribute(timestampFieldName)), Milliseconds(windowSize)))
                                   .byKey(Attribute("f2_right"))
                                   .apply(Sum(Attribute("fieldForSum2"))))
                     .where(Attribute("f2_left") == Attribute("f2_right"))
                     .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);
    const auto resultRecords = runQueryWithCsvFiles<ResultRecord>({{joinParams.inputSchemas[0], csvFileParams.inputCsvFiles[0]},
                                                                   {joinParams.inputSchemas[1], csvFileParams.inputCsvFiles[1]}},
                                                                  expectedSinkVector.size(),
                                                                  testSink,
                                                                  query);

    // Checking for correctness
    ASSERT_EQ(resultRecords.size(), expectedSinkVector.size());
    EXPECT_THAT(resultRecords, ::testing::UnorderedElementsAreArray(expectedSinkVector));
}

INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQueryExecutionTest,
                        JOIN_STRATEGIES_WINDOW_STRATEGIES,
                        [](const testing::TestParamInfo<StreamJoinQueryExecutionTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param)));
                        });

}// namespace NES::Runtime::Execution
