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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
#include <type_traits>
#include <utility>

namespace NES {

TestHarness::TestHarness(Query queryWithoutSink,
                         uint16_t restPort,
                         uint16_t rpcPort,
                         std::filesystem::path testHarnessResourcePath,
                         uint64_t memSrcFrequency,
                         uint64_t memSrcNumBuffToProcess)
    : queryWithoutSink(std::make_shared<Query>(std::move(queryWithoutSink))), coordinatorHostAddress("127.0.0.1"),
      restPort(restPort), rpcPort(rpcPort), useNewRequestExecutor(false), memSrcFrequency(memSrcFrequency),
      memSrcNumBuffToProcess(memSrcNumBuffToProcess), bufferSize(4096), physicalSourceCount(0), topologyId(WorkerId(1)),
      joinStrategy(QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN),
      windowingStrategy(QueryCompilation::WindowingStrategy::SLICING), validationDone(false), topologySetupDone(false),
      filePath(testHarnessResourcePath / "testHarness.csv"), bufferManager(std::make_shared<Runtime::BufferManager>()) {}

TestHarness& TestHarness::addLogicalSource(const std::string& logicalSourceName, const SchemaPtr& schema) {
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    this->logicalSources.emplace_back(logicalSource);
    return *this;
}

TestHarness& TestHarness::setJoinStrategy(QueryCompilation::StreamJoinStrategy& newJoinStrategy) {
    this->joinStrategy = newJoinStrategy;
    return *this;
}

TestHarness& TestHarness::setWindowingStrategy(QueryCompilation::WindowingStrategy& newWindowingStrategy) {
    this->windowingStrategy = newWindowingStrategy;
    return *this;
}

void TestHarness::checkAndAddLogicalSources() {

    for (const auto& logicalSource : logicalSources) {

        auto logicalSourceName = logicalSource->getLogicalSourceName();
        auto schema = logicalSource->getSchema();

        // Check if logical source already exists
        auto sourceCatalog = nesCoordinator->getSourceCatalog();
        if (!sourceCatalog->containsLogicalSource(logicalSourceName)) {
            NES_TRACE("TestHarness: logical source does not exist in the source catalog, adding a new logical source {}",
                      logicalSourceName);
            sourceCatalog->addLogicalSource(logicalSourceName, schema);
        } else {
            // Check if it has the same schema
            if (!sourceCatalog->getSchemaForLogicalSource(logicalSourceName)->equals(schema, true)) {
                NES_TRACE("TestHarness: logical source {} exists in the source catalog with different schema, replacing it "
                          "with a new schema",
                          logicalSourceName);
                sourceCatalog->removeLogicalSource(logicalSourceName);
                sourceCatalog->addLogicalSource(logicalSourceName, schema);
            }
        }
    }
}

TestHarness& TestHarness::attachWorkerWithMemorySourceToWorkerWithId(const std::string& logicalSourceName,
                                                                     WorkerId parentId,
                                                                     WorkerConfigurationPtr workerConfiguration) {
    workerConfiguration->parentId = parentId;
#ifdef TFDEF
    workerConfiguration->isTensorflowSupported = true;
#endif// TFDEF
    std::string physicalSourceName = getNextPhysicalSourceName();
    auto workerId = getNextTopologyId();
    auto testHarnessWorkerConfiguration =
        TestHarnessWorkerConfiguration::create(workerConfiguration,
                                               logicalSourceName,
                                               physicalSourceName,
                                               TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::MemorySource,
                                               workerId);
    testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
    return *this;
}

TestHarness& TestHarness::attachWorkerWithMemorySourceToCoordinator(const std::string& logicalSourceName) {
    //We are assuming coordinator will start with id 1
    return attachWorkerWithMemorySourceToWorkerWithId(std::move(logicalSourceName), WorkerId(1));
}

TestHarness& TestHarness::attachWorkerWithLambdaSourceToCoordinator(PhysicalSourceTypePtr physicalSource,
                                                                    WorkerConfigurationPtr workerConfiguration) {
    //We are assuming coordinator will start with id 1
    workerConfiguration->parentId = WorkerId(1);
    auto workerId = getNextTopologyId();
    auto testHarnessWorkerConfiguration =
        TestHarnessWorkerConfiguration::create(workerConfiguration,
                                               physicalSource->getLogicalSourceName(),
                                               physicalSource->getPhysicalSourceName(),
                                               TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::LambdaSource,
                                               workerId);
    testHarnessWorkerConfiguration->setPhysicalSourceType(physicalSource);
    testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
    return *this;
}

TestHarness& TestHarness::attachWorkerWithCSVSourceToWorkerWithId(const CSVSourceTypePtr& csvSourceType, WorkerId parentId) {
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSourceTypes.add(csvSourceType);
    workerConfiguration->parentId = parentId;
    auto workerId = getNextTopologyId();
    auto testHarnessWorkerConfiguration =
        TestHarnessWorkerConfiguration::create(workerConfiguration,
                                               csvSourceType->getLogicalSourceName(),
                                               csvSourceType->getPhysicalSourceName(),
                                               TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::CSVSource,
                                               workerId);
    testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
    return *this;
}

TestHarness& TestHarness::attachWorkerWithCSVSourceToCoordinator(const CSVSourceTypePtr& csvSourceType) {
    //We are assuming coordinator will start with id 1
    return attachWorkerWithCSVSourceToWorkerWithId(csvSourceType, WorkerId(1));
}

TestHarness& TestHarness::attachWorkerToWorkerWithId(WorkerId parentId) {

    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->parentId = parentId;
    auto workerId = getNextTopologyId();
    auto testHarnessWorkerConfiguration = TestHarnessWorkerConfiguration::create(workerConfiguration, workerId);
    testHarnessWorkerConfigurations.emplace_back(testHarnessWorkerConfiguration);
    return *this;
}

TestHarness& TestHarness::attachWorkerToCoordinator() {
    //We are assuming coordinator will start with id 1
    return attachWorkerToWorkerWithId(WorkerId(1));
}
uint64_t TestHarness::getWorkerCount() { return testHarnessWorkerConfigurations.size(); }

TestHarness& TestHarness::validate() {
    validationDone = true;
    if (this->logicalSources.empty()) {
        throw Exceptions::RuntimeException(
            "No Logical source defined. Please make sure you add logical source while defining up test harness.");
    }

    if (testHarnessWorkerConfigurations.empty()) {
        throw Exceptions::RuntimeException("TestHarness: No worker added to the test harness.");
    }

    uint64_t sourceCount = 0;
    for (const auto& workerConf : testHarnessWorkerConfigurations) {
        if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::MemorySource
            && workerConf->getRecords().empty()) {
            throw Exceptions::RuntimeException("TestHarness: No Record defined for Memory Source with logical source Name: "
                                               + workerConf->getLogicalSourceName() + " and Physical source name : "
                                               + workerConf->getPhysicalSourceName() + ". Please add data to the test harness.");
        }

        if (workerConf->getSourceType() == TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::CSVSource
            || workerConf->getSourceType() == TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::MemorySource
            || workerConf->getSourceType() == TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::LambdaSource) {
            sourceCount++;
        }
    }

    if (sourceCount == 0) {
        throw Exceptions::RuntimeException("TestHarness: No Physical source defined in the test harness.");
    }
    return *this;
}

PhysicalSourceTypePtr TestHarness::createPhysicalSourceOfLambdaType(TestHarnessWorkerConfigurationPtr workerConf) {
    auto logicalSourceName = workerConf->getLogicalSourceName();
    auto found =
        std::find_if(logicalSources.begin(), logicalSources.end(), [logicalSourceName](const LogicalSourcePtr& logicalSource) {
            return logicalSource->getLogicalSourceName() == logicalSourceName;
        });

    if (found == logicalSources.end()) {
        throw Exceptions::RuntimeException("Unable to find logical source with name " + logicalSourceName
                                           + ". Make sure you are adding a logical source with the name to the test harness.");
    }
    return workerConf->getPhysicalSourceType();
};

PhysicalSourceTypePtr TestHarness::createPhysicalSourceOfMemoryType(TestHarnessWorkerConfigurationPtr workerConf) {
    // create and populate memory source
    auto currentSourceNumOfRecords = workerConf->getRecords().size();
    auto logicalSourceName = workerConf->getLogicalSourceName();

    SchemaPtr schema;
    for (const auto& logicalSource : logicalSources) {
        if (logicalSource->getLogicalSourceName() == logicalSourceName) {
            schema = logicalSource->getSchema();
        }
    }

    if (!schema) {
        throw Exceptions::RuntimeException("Unable to find logical source with name " + logicalSourceName
                                           + ". Make sure you are adding a logical source with the name to the test harness.");
    }

    auto tupleSize = schema->getSchemaSizeInBytes();
    NES_DEBUG("Tuple Size: {}", tupleSize);
    NES_DEBUG("currentSourceNumOfRecords: {}", currentSourceNumOfRecords);
    auto memAreaSize = currentSourceNumOfRecords * tupleSize;
    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));

    auto currentRecords = workerConf->getRecords();
    for (std::size_t j = 0; j < currentSourceNumOfRecords; ++j) {
        memcpy(&memArea[tupleSize * j], currentRecords.at(j), tupleSize);
    }

    memSrcNumBuffToProcess =
        std::ceil(static_cast<double>(memAreaSize) / workerConf->getWorkerConfiguration()->bufferSizeInBytes);
    NES_DEBUG("memSrcNumBuffToProcess = {} currentSourceNumOfRecords = {}", memSrcNumBuffToProcess, currentSourceNumOfRecords);

    auto memorySourceType = MemorySourceType::create(logicalSourceName,
                                                     workerConf->getPhysicalSourceName(),
                                                     memArea,
                                                     memAreaSize,
                                                     memSrcNumBuffToProcess,
                                                     memSrcFrequency,
                                                     GatheringMode::INTERVAL_MODE);
    return memorySourceType;
};

SchemaPtr TestHarness::getOutputSchema() {
    auto requestHandlerService = nesCoordinator->getRequestHandlerService();
    auto queryCatalog = nesCoordinator->getQueryCatalog();
    return queryPlan->getSinkOperators()[0]->getOutputSchema();
}

TestHarness&
TestHarness::runQuery(uint64_t numberOfRecordsToExpect, const std::string& placementStrategyName, uint64_t testTimeoutInSeconds) {
    if (!topologySetupDone || !validationDone) {
        throw Exceptions::RuntimeException(
            "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
    }

    auto queryCatalog = nesCoordinator->getQueryCatalog();

    // Add a file sink to the query and enqueue it.
    addFileSink();
    validateAndQueueAddQueryRequest(magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyName).value());

    // Wait for query completion
    if (!TestUtils::waitForQueryToStart(queryId, queryCatalog)) {
        NES_THROW_RUNTIME_ERROR("TestHarness: waitForQueryToStart returns false");
    }

    if (!TestUtils::checkOutputContentLengthOrTimeout(queryId,
                                                      queryCatalog,
                                                      numberOfRecordsToExpect,
                                                      filePath,
                                                      testTimeoutInSeconds)) {
        NES_THROW_RUNTIME_ERROR("TestHarness: checkOutputContentLengthOrTimeout returns false, number of bytes to expect = "
                                << numberOfRecordsToExpect);
    }

    if (!TestUtils::checkStoppedOrTimeout(queryId, queryCatalog)) {
        NES_THROW_RUNTIME_ERROR("TestHarness: checkStoppedOrTimeout returns false for query with id= " << queryId);
    }

    queryPlan = queryCatalog->getCopyOfExecutedQueryPlan(queryId);
    executionPlan = nesCoordinator->getGlobalExecutionPlan();
    NES_DEBUG("TestHarness: ExecutedQueryPlan: {}", queryPlan->toString());

    for (const auto& worker : testHarnessWorkerConfigurations) {
        worker->getNesWorker()->stop(false);
    }
    nesCoordinator->stopCoordinator(false);
    NES_DEBUG("TestHarness: GlobalExecutionPlan: {}", nesCoordinator->getGlobalExecutionPlan()->getAsString());

    return *this;
}

TestHarness& TestHarness::addFileSink() {
    queryWithoutSink->sink(FileSinkDescriptor::create(filePath, "CSV_FORMAT", appendMode));
    return *this;
}

TestHarness& TestHarness::validateAndQueueAddQueryRequest(const Optimizer::PlacementStrategy& placementStrategy) {
    auto requestHandlerService = nesCoordinator->getRequestHandlerService();
    queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryWithoutSink->getQueryPlan(), placementStrategy);
    return *this;
}

bool TestHarness::checkFailedOrTimeout() const {
    return TestUtils::checkFailedOrTimeout(queryId, nesCoordinator->getQueryCatalog());
}

std::vector<Runtime::MemoryLayouts::TestTupleBuffer> TestHarness::getOutput() {
    const auto schema = queryPlan->getSinkOperators()[0]->getOutputSchema();
    auto tupleBuffers = TestUtils::createExpectedBuffersFromCsv(filePath, schema, bufferManager, true);
    return TestUtils::createTestTupleBuffers(tupleBuffers, schema);
}

TestHarness& TestHarness::setupTopology(std::function<void(CoordinatorConfigurationPtr)> crdConfigFunctor,
                                        const std::vector<nlohmann::json>& distributionList) {
    if (!validationDone) {
        NES_THROW_RUNTIME_ERROR("Please call validate before calling setup.");
    }

    //Start Coordinator
    auto coordinatorConfiguration = CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->coordinatorHost = coordinatorHostAddress;
    coordinatorConfiguration->restPort = restPort;
    coordinatorConfiguration->rpcPort = rpcPort;

    coordinatorConfiguration->worker.queryCompiler.queryCompilerDumpMode = QueryCompilation::DumpMode::CONSOLE;
    coordinatorConfiguration->worker.queryCompiler.windowingStrategy = windowingStrategy;
    coordinatorConfiguration->worker.queryCompiler.joinStrategy = joinStrategy;

    crdConfigFunctor(coordinatorConfiguration);

    nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    auto coordinatorRPCPort = nesCoordinator->startCoordinator(/**blocking**/ false);
    //Add all logical sources
    checkAndAddLogicalSources();

    std::vector<WorkerId> workerIds;

    for (auto& workerConf : testHarnessWorkerConfigurations) {

        //Fetch the worker configuration
        auto workerConfiguration = workerConf->getWorkerConfiguration();

        workerConfiguration->queryCompiler.queryCompilerDumpMode = QueryCompilation::DumpMode::CONSOLE;
        workerConfiguration->queryCompiler.windowingStrategy = windowingStrategy;
        workerConfiguration->queryCompiler.joinStrategy = joinStrategy;

        //Set ports at runtime
        workerConfiguration->coordinatorPort = coordinatorRPCPort;
        workerConfiguration->coordinatorHost = coordinatorHostAddress;

        switch (workerConf->getSourceType()) {
            case TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::MemorySource: {
                auto physicalSource = createPhysicalSourceOfMemoryType(workerConf);
                workerConfiguration->physicalSourceTypes.add(physicalSource);
                break;
            }
            case TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType::LambdaSource: {
                auto physicalSource = createPhysicalSourceOfLambdaType(workerConf);
                workerConfiguration->physicalSourceTypes.add(physicalSource);
                break;
            }
            default: break;
        }

        NesWorkerPtr nesWorker = std::make_shared<NesWorker>(std::move(workerConfiguration));
        nesWorker->start(/**blocking**/ false, /**withConnect**/ true);
        workerIds.emplace_back(nesWorker->getWorkerId());

        //We are assuming that coordinator has a node id 1
        nesWorker->replaceParent(WorkerId(1), nesWorker->getWorkerConfiguration()->parentId.getValue());

        //Add Nes Worker to the configuration.
        //Note: this is required to stop the NesWorker at the end of the test
        workerConf->setQueryStatusListener(nesWorker);
    }

    auto topology = nesCoordinator->getTopology();

    auto start_timestamp = std::chrono::system_clock::now();

    for (const auto& workerId : workerIds) {
        while (!topology->nodeWithWorkerIdExists(workerId)) {
            if (std::chrono::system_clock::now() > start_timestamp + SETUP_TIMEOUT_IN_SEC) {
                NES_THROW_RUNTIME_ERROR("TestHarness: Unable to find setup topology in given timeout.");
            }
        }
    }

    // set up the key distribution
    auto sourceCatalog = nesCoordinator->getSourceCatalog();
    if (!distributionList.empty()) {
        for (auto jsonFields : distributionList) {
            auto workerId = WorkerId(jsonFields["topologyNodeId"]);
            std::string logSourceName = jsonFields["logicalSource"];
            std::string phSourceName = jsonFields["physicalSource"];
            std::string fieldName = jsonFields["fieldName"];
            std::string value = jsonFields["value"];

            Catalogs::Source::SourceCatalogEntryPtr catalogEntry;
            std::vector<Catalogs::Source::SourceCatalogEntryPtr> physicalSources =
                sourceCatalog->getPhysicalSources(logSourceName);
            for (const auto& phSource : physicalSources) {
                if (phSource->getLogicalSource()->getLogicalSourceName() == logSourceName
                    && phSource->getPhysicalSource()->getPhysicalSourceName() == phSourceName
                    && phSource->getTopologyNodeId() == workerId) {
                    catalogEntry = phSource;
                    std::set<uint64_t> vals = {std::stoull(value)};
                    sourceCatalog->getKeyDistributionMap()[catalogEntry] = {vals};
                    break;
                }
            }
        }
    } else {
        NES_DEBUG("TestHarness: Key distribution list is empty.")
    }
    topologySetupDone = true;
    return *this;
}

TopologyPtr TestHarness::getTopology() {

    if (!validationDone && !topologySetupDone) {
        throw Exceptions::RuntimeException(
            "Make sure to call first validate() and then setupTopology() to the test harness before checking the output");
    }
    return nesCoordinator->getTopology();
};

const QueryPlanPtr& TestHarness::getQueryPlan() const { return queryPlan; }

const Optimizer::GlobalExecutionPlanPtr& TestHarness::getExecutionPlan() const { return executionPlan; }

std::string TestHarness::getNextPhysicalSourceName() {
    physicalSourceCount++;
    return std::to_string(physicalSourceCount);
}

WorkerId TestHarness::getNextTopologyId() {
    topologyId = WorkerId(topologyId.getRawValue() + 1);
    return topologyId;
}
Runtime::BufferManagerPtr TestHarness::getBufferManager() const { return bufferManager; }

TestHarness& TestHarness::setOutputFilePath(const std::string& newOutputFilePath) {
    this->filePath = newOutputFilePath;
    return *this;
}

TestHarness& TestHarness::setAppendMode(const std::string_view newAppendMode) {
    this->appendMode = newAppendMode;
    return *this;
}

TestHarness& TestHarness::stopCoordinatorAndWorkers() {
    for (const auto& worker : testHarnessWorkerConfigurations) {
        worker->getNesWorker()->stop(false);
    }
    nesCoordinator->stopCoordinator(false);
    return *this;
}

}// namespace NES
