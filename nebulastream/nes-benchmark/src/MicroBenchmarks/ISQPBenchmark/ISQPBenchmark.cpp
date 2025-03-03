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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkPropertyEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/Core.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <z3++.h>

using namespace NES;
using namespace NES::Benchmark;
using std::filesystem::directory_iterator;

std::chrono::nanoseconds Runtime;

class ErrorHandler : public Exceptions::ErrorListener {
  public:
    virtual void onFatalError(int signalNumber, std::string callstack) override {
        if (callstack.empty()) {
            std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] ";
        } else {
            std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                      << callstack;
        }
    }

    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        if (callstack.empty()) {
            std::cout << "onFatalException: exception=[" << exception->what() << "] ";
        } else {
            std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
        }
    }
};

/**
 * @brief Set up the physical sources for the benchmark
 * @param leafWorkerIds: leaf worker ids
 */
void setupSources(std::vector<WorkerId> leafWorkerIds, RequestHandlerServicePtr requestHandlerService) {

    //register logical stream with different schema
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("a", BasicType::UINT64)
                                 ->addField("b", BasicType::UINT64)
                                 ->addField("c", BasicType::UINT64)
                                 ->addField("d", BasicType::UINT64)
                                 ->addField("e", BasicType::UINT64)
                                 ->addField("f", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("g", BasicType::UINT64)
                                 ->addField("h", BasicType::UINT64)
                                 ->addField("i", BasicType::UINT64)
                                 ->addField("j", BasicType::UINT64)
                                 ->addField("k", BasicType::UINT64)
                                 ->addField("l", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema3 = NES::Schema::create()
                                 ->addField("m", BasicType::UINT64)
                                 ->addField("n", BasicType::UINT64)
                                 ->addField("o", BasicType::UINT64)
                                 ->addField("p", BasicType::UINT64)
                                 ->addField("q", BasicType::UINT64)
                                 ->addField("r", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema4 = NES::Schema::create()
                                 ->addField("s", BasicType::UINT64)
                                 ->addField("t", BasicType::UINT64)
                                 ->addField("u", BasicType::UINT64)
                                 ->addField("v", BasicType::UINT64)
                                 ->addField("w", BasicType::UINT64)
                                 ->addField("x", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    //Add the logical and physical stream to the stream catalog such that each leaf has two distinct sources attached
    for (const auto& leafWorkerId : leafWorkerIds) {
        uint16_t noOfPhySourcePerWorker = 32;
        for (uint16_t counter = 1; counter <= noOfPhySourcePerWorker; counter++) {
            const auto& logicalSourceName = "example" + leafWorkerId.toString() + "-" + std::to_string(counter);
            auto physicalSourceName = "phy_" + logicalSourceName;
            std::vector<RequestProcessor::PhysicalSourceDefinition> additions;
            additions.emplace_back(logicalSourceName, physicalSourceName);
            requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema3);
            requestHandlerService->queueRegisterPhysicalSourceRequest(additions, leafWorkerId);
        }
    }
}

/**
 * @brief This method construct a three layer infrastructure with (Cloud)->(Fog)->(IoT/Edge)
 * @param numOfRootNodes : number of root nodes
 * @param numOfIntermediateNodes : number of intermediate nodes
 * @param numOfSourceNodes : number of source nodes
 * @param requestHandlerService : request handler to launch ISQP requests
 * @param topology : the topology
 * @param globalExecutionPlan : the global execution plan
 */
void setupTopology(uint16_t numOfRootNodes,
                   uint16_t numOfIntermediateNodes,
                   uint16_t numOfSourceNodes,
                   RequestHandlerServicePtr requestHandlerService,
                   TopologyPtr topology,
                   Optimizer::GlobalExecutionPlanPtr globalExecutionPlan) {

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //1. Compute ISQP request to register new root nodes (Cloud Layer)
    std::vector<RequestProcessor::ISQPEventPtr> rootISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < numOfRootNodes; counter++) {
        rootISQPAddNodeEvents.emplace_back(RequestProcessor::ISQPAddNodeEvent::create(RequestProcessor::WorkerType::CLOUD,
                                                                                      INVALID_WORKER_NODE_ID,
                                                                                      "localhost",
                                                                                      0,
                                                                                      0,
                                                                                      UINT16_MAX,
                                                                                      properties));
    }
    requestHandlerService->queueISQPRequest(rootISQPAddNodeEvents);

    //2. Compute ISQP request to register intermediate nodes (Fog layer)
    std::vector<RequestProcessor::ISQPEventPtr> intermediateISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < numOfIntermediateNodes; counter++) {
        intermediateISQPAddNodeEvents.emplace_back(
            RequestProcessor::ISQPAddNodeEvent::create(RequestProcessor::WorkerType::SENSOR,
                                                       INVALID_WORKER_NODE_ID,
                                                       "localhost",
                                                       0,
                                                       0,
                                                       UINT16_MAX,
                                                       properties));
    }
    requestHandlerService->queueISQPRequest(intermediateISQPAddNodeEvents);

    //3. Compute ISQP request to register leaf nodes (IoT Layer)
    std::vector<RequestProcessor::ISQPEventPtr> leafISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < numOfSourceNodes; counter++) {
        leafISQPAddNodeEvents.emplace_back(RequestProcessor::ISQPAddNodeEvent::create(RequestProcessor::WorkerType::SENSOR,
                                                                                      INVALID_WORKER_NODE_ID,
                                                                                      "localhost",
                                                                                      0,
                                                                                      0,
                                                                                      UINT16_MAX,
                                                                                      properties));
    }
    requestHandlerService->queueISQPRequest(leafISQPAddNodeEvents);

    //4. Record the root node worker Ids
    std::vector<WorkerId> rootWorkerIds = topology->getRootWorkerNodeIds();
    for (const auto& rootISQPAddNodeEvent : rootISQPAddNodeEvents) {
        const auto& response = rootISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        rootWorkerIds.emplace_back(workerId);
    }

    //5. Record the intermediate node worker Ids
    std::vector<WorkerId> intermediateWorkerIds;
    for (const auto& intermediateISQPAddNodeEvent : intermediateISQPAddNodeEvents) {
        const auto& response =
            intermediateISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        intermediateWorkerIds.emplace_back(workerId);
    }

    //6. Record the leaf node worker Ids
    std::vector<WorkerId> leafWorkerIds;
    for (const auto& leafISQPAddNodeEvent : leafISQPAddNodeEvents) {
        const auto& response = leafISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        leafWorkerIds.emplace_back(workerId);
    }

    //7. Remove connection between leaf and root nodes
    std::vector<RequestProcessor::ISQPEventPtr> linkRemoveEvents;
    for (const auto& rootWorkerId : rootWorkerIds) {
        for (const auto& leafWorkerId : leafWorkerIds) {
            auto linkRemoveEvent = RequestProcessor::ISQPRemoveLinkEvent::create(rootWorkerId, leafWorkerId);
            linkRemoveEvents.emplace_back(linkRemoveEvent);
        }
    }
    requestHandlerService->queueISQPRequest(linkRemoveEvents);

    //8. Add connection between leaf and intermediate nodes
    std::vector<RequestProcessor::ISQPEventPtr> linkAddEvents;
    uint16_t leafNodeCounter = 0;
    for (const auto& intermediateWorkerId : intermediateWorkerIds) {
        uint16_t connectivityCounter = 1;
        for (; leafNodeCounter < leafWorkerIds.size(); leafNodeCounter++) {
            auto linkAddEvent = RequestProcessor::ISQPAddLinkEvent::create(intermediateWorkerId, leafWorkerIds[leafNodeCounter]);
            linkAddEvents.emplace_back(linkAddEvent);
            connectivityCounter++;
            if (connectivityCounter > leafWorkerIds.size() / intermediateWorkerIds.size()) {
                leafNodeCounter++;
                break;
            }
        }
    }
    requestHandlerService->queueISQPRequest(linkAddEvents);

    //9. Add link properties between root and intermediate nodes
    std::vector<RequestProcessor::ISQPEventPtr> addLinkPropertyEvents;
    for (const auto& rootWorkerId : rootWorkerIds) {
        for (const auto& intermediateWorkerId : intermediateWorkerIds) {
            auto addLinkPropertyEvent =
                RequestProcessor::ISQPAddLinkPropertyEvent::create(rootWorkerId, intermediateWorkerId, 1, 1);
            addLinkPropertyEvents.emplace_back(addLinkPropertyEvent);
        }
    }

    //10. Add link properties properties between leaf and intermediate nodes
    leafNodeCounter = 0;
    for (const auto& intermediateWorkerId : intermediateWorkerIds) {
        uint16_t connectivityCounter = 1;
        for (; leafNodeCounter < leafWorkerIds.size(); leafNodeCounter++) {
            auto addLinkPropertyEvent =
                RequestProcessor::ISQPAddLinkPropertyEvent::create(intermediateWorkerId, leafWorkerIds[leafNodeCounter], 1, 1);
            addLinkPropertyEvents.emplace_back(addLinkPropertyEvent);
            connectivityCounter++;
            if (connectivityCounter > leafWorkerIds.size() / intermediateWorkerIds.size()) {
                leafNodeCounter++;
                break;
            }
        }
    }
    requestHandlerService->queueISQPRequest(addLinkPropertyEvents);

    //11. Create execution Nodes
    for (const auto& workerId : rootWorkerIds) {
        auto lockedTopologyNode = topology->lockTopologyNode(workerId);
        globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    }
    for (const auto& workerId : intermediateWorkerIds) {
        auto lockedTopologyNode = topology->lockTopologyNode(workerId);
        globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    }
    for (const auto& workerId : leafWorkerIds) {
        auto lockedTopologyNode = topology->lockTopologyNode(workerId);
        globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    }

    //12. create logical and physical sources
    setupSources(leafWorkerIds, requestHandlerService);
};

/**
  * @brief setup the topology ((Cloud)->(Fog)->(IoT/Edge)) and physical sources to run the experiment
  * @param numOfRootNodes : number of root nodes
  * @param numOfIntermediateNodes : number of intermediate nodes
  * @param numOfSourceNodes : number of source nodes
  * @param requestHandlerService : request handler to launch ISQP requests
  * @param topology : the topology
  * @param globalExecutionPlan : the global execution plan
  */
void setUp(uint16_t numOfRootNodes,
           uint16_t numOfIntermediateNodes,
           uint16_t numOfSourceNodes,
           RequestHandlerServicePtr requestHandlerService,
           TopologyPtr topology,
           Optimizer::GlobalExecutionPlanPtr globalExecutionPlan) {
    setupTopology(numOfRootNodes, numOfIntermediateNodes, numOfSourceNodes, requestHandlerService, topology, globalExecutionPlan);
}

/**
 * @brief Split string by delimiter
 * @param input : string to split
 * @param delim : delimiter
 * @return  vector of split string
 */
std::vector<std::string> split(const std::string input, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(input);
    std::string item;
    while (getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

/**
 * @brief Load provided configuration file
 * @param filePath : location of the configuration file
 */
Yaml::Node loadConfigFromYAMLFile(const std::string& filePath) {

    NES_INFO("BenchmarkIncrementalPlacement: Using config file with path: {} .", filePath);
    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());
            return config;
        } catch (std::exception& e) {
            NES_ERROR("BenchmarkIncrementalPlacement: Error while initializing configuration parameters from YAML file. {}",
                      e.what());
            throw e;
        }
    }
    NES_THROW_RUNTIME_ERROR("BenchmarkIncrementalPlacement: No file path was provided or file could not be found at the path: "
                            + filePath);
}

void compileQuery(const std::string& stringQuery,
                  uint64_t id,
                  const std::shared_ptr<QueryParsingService>& queryParsingService,
                  std::promise<QueryPlanPtr> promise) {
    auto queryplan = queryParsingService->createQueryFromCodeString(stringQuery);
    queryplan->setQueryId(QueryId(id));
    promise.set_value(queryplan);
}

/**
 * @brief This benchmarks time taken in the preparation of Global Query Plan after merging @param{NO_OF_QUERIES_TO_SEND} number of queries.
 */
int main(int argc, const char* argv[]) {

    auto listener = std::make_shared<ErrorHandler>();
    Exceptions::installGlobalErrorListener(listener);

    NES::Logger::setupLogging("BenchmarkIncrementalPlacement.log", NES::LogLevel::LOG_NONE);
    std::cout << "Setup BenchmarkIncrementalPlacement test class." << std::endl;

    //Load all command line arguments
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(std::pair<std::string, std::string>(
            std::string(argv[i]).substr(0, std::string(argv[i]).find("=")),
            std::string(argv[i]).substr(std::string(argv[i]).find("=") + 1, std::string(argv[i]).length() - 1)));
    }

    // Location of the configuration file
    auto configPath = commandLineParams.find("--configPath");

    Yaml::Node configs;
    //Load the configuration file
    if (configPath != commandLineParams.end()) {
        configs = loadConfigFromYAMLFile(configPath->second);
    } else {
        NES_ERROR("Configuration file is not provided. Please use the option --configPath");
        return -1;
    }

    //Fetch base benchmark configurations
    auto logLevel = configs["LogLevel"].As<std::string>();
    auto numberOfRun = configs["NumOfRuns"].As<uint16_t>();
    auto startupSleepInterval = configs["StartupSleepIntervalInSeconds"].As<uint16_t>();
    auto initialQueries = configs["InitialQueries"].As<uint16_t>();
    NES::Logger::setupLogging("ISQPBenchmark.log", magic_enum::enum_cast<LogLevel>(logLevel).value());
    //Load queries from the query set location and run the benchmark
    auto querySetLocation = configs["QuerySetLocation"].As<std::string>();

    //Load individual query set from the query set location and run the benchmark
    for (const auto& file : directory_iterator(querySetLocation)) {

        auto fileName = file.path().filename().string();
        if (fileName.find("_done") != std::string::npos) {
            std::cout << "Skip processing event set" << fileName << std::endl;
            continue;
        }

        std::cout << "Processing the change event set" << fileName << std::endl;

        //Read the input query set and load the query string in the queries vector
        std::ifstream infile(file.path());
        std::vector<std::string> queries;
        std::string line;
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            queries.emplace_back(line);
        }

        if (queries.empty()) {
            NES_THROW_RUNTIME_ERROR("Unable to find any query");
        }

        //using thread pool to parallelize the compilation of string queries and string them in an array of query objects
        const uint32_t numOfQueries = queries.size();
        QueryPlanPtr queryObjects[numOfQueries];

        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);

        //If no available thread then set number of threads to 1
        uint64_t numThreads = std::thread::hardware_concurrency();
        if (numThreads == 0) {
            NES_WARNING("No available threads. Going to use only 1 thread for parsing input queries.");
            numThreads = 1;
        }
        std::cout << "Using " << numThreads << " of threads for parallel parsing." << std::endl;

        uint64_t queryNum = 0;
        //Work till all queries are not parsed
        while (queryNum < numOfQueries) {
            std::vector<std::future<QueryPlanPtr>> futures;
            std::vector<std::thread> threadPool(numThreads);
            uint64_t threadNum;
            //Schedule queries to be parsed with #numThreads parallelism
            for (threadNum = 0; threadNum < numThreads; threadNum++) {
                //If no more query to parse
                if (queryNum >= numOfQueries) {
                    break;
                }
                //Schedule thread for execution and pass a promise
                std::promise<QueryPlanPtr> promise;
                //Store the future, schedule the thread, and increment the query count
                futures.emplace_back(promise.get_future());
                threadPool.emplace_back(
                    std::thread(compileQuery, queries[queryNum], queryNum + 1, queryParsingService, std::move(promise)));
                queryNum++;
            }

            //Wait for all unfinished threads
            for (auto& item : threadPool) {
                if (item.joinable()) {// if thread is not finished yet
                    item.join();
                }
            }

            //Fetch the parsed query from all threads
            for (uint64_t futureNum = 0; futureNum < threadNum; futureNum++) {
                auto query = futures[futureNum].get();
                auto queryID = query->getQueryId();
                queryObjects[queryID.getRawValue() - 1] = query;//Add the parsed query to the (queryID - 1)th index
            }
        }

        std::cout << "Parsed all queries." << std::endl;

        std::stringstream aggregatedBenchmarkOutput;
        aggregatedBenchmarkOutput << "File_Name,ConfigNum,PlacementRule,IncrementalPlacement,PlacementAmendmentThreadCount,"
                                     "PlacementAmendmentMode,NumOfAddQuery,NumOfRemoveQuery,BatchSize,Run_Num,Batch_Num,Start_"
                                     "Time,End_Time,Total_Run_Time"
                                  << std::endl;

        std::stringstream detailedBenchmarkOutput;
        detailedBenchmarkOutput << "File_Name,ConfigNum,PlacementRule,IncrementalPlacement,"
                                   "PlacementAmendmentThreadCount,PlacementAmendmentMode,"
                                   "BatchSize,RunNum,NumOfAddQuery,NumOfRemoveQuery,QueryNum,BatchNum,batchEventTime,"
                                   "batchProcessingStartTime,batchAmendmentStartTime,batchProcessingEndTime,"
                                   "batchFailureCount,batchAffectedSQPCount"
                                << std::endl;

        //Perform benchmark for each run configuration
        auto runConfig = configs["RunConfig"];
        for (auto entry = runConfig.Begin(); entry != runConfig.End(); entry++) {
            auto node = (*entry).second;
            auto placementStrategy = node["QueryPlacementStrategy"].As<std::string>();
            auto incrementalPlacement = node["IncrementalPlacement"].As<bool>();
            auto batchSize = node["BatchSize"].As<uint16_t>();
            auto numOfRemoveQueries = node["NumOfRemoveQueries"].As<uint16_t>();
            auto numOfAddQueries = node["NumOfAddQueries"].As<uint16_t>();
            std::cout << "NumOfRemoveQueries:" << numOfRemoveQueries << ", NumOfAddQueries:" << numOfAddQueries << ", BatchSize"
                      << batchSize;
            auto placementAmendmentThreadCount = node["PlacementAmendmentThreadCount"].As<uint32_t>();
            auto configNum = node["ConfNum"].As<uint32_t>();
            auto placementAmendmentMode = node["PlacementAmendmentMode"].As<std::string>();
            auto coordinatorConfiguration = CoordinatorConfiguration::createDefault();
            coordinatorConfiguration->logLevel = magic_enum::enum_cast<LogLevel>(logLevel).value();
            //Set optimizer configuration
            OptimizerConfiguration optimizerConfiguration;
            optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule;
            optimizerConfiguration.enableIncrementalPlacement = incrementalPlacement;
            optimizerConfiguration.placementAmendmentThreadCount = placementAmendmentThreadCount;
            optimizerConfiguration.placementAmendmentMode =
                magic_enum::enum_cast<Optimizer::PlacementAmendmentMode>(placementAmendmentMode).value();
            coordinatorConfiguration->optimizer = optimizerConfiguration;

            for (uint32_t run = 0; run < numberOfRun; run++) {

                std::this_thread::sleep_for(std::chrono::seconds(startupSleepInterval));
                auto nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfiguration);
                nesCoordinator->startCoordinator(false);
                auto requestHandlerService = nesCoordinator->getRequestHandlerService();
                auto topology = nesCoordinator->getTopology();
                auto globalExecutionPlan = nesCoordinator->getGlobalExecutionPlan();
                std::cout << "Setting up the topology." << std::endl;
                //Setup topology and source catalog
                setUp(63, 64, 640, requestHandlerService, topology, globalExecutionPlan);

                auto placement = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategy).value();

                // Setup system with initially running query plans
                std::vector<QueryId> potentialCandidatesForRemoval;
                std::vector<RequestProcessor::ISQPEventPtr> initialISQPEvents;
                uint64_t currentIndex;
                for (currentIndex = 0; currentIndex < initialQueries; currentIndex++) {
                    auto queryPlan = queryObjects[currentIndex];
                    potentialCandidatesForRemoval.emplace_back(queryPlan->getQueryId());
                    initialISQPEvents.emplace_back(RequestProcessor::ISQPAddQueryEvent::create(queryPlan->copy(), placement));
                }
                std::cout << "_______________________________Placing initial queries" << std::endl;
                requestHandlerService->queueISQPRequest(initialISQPEvents);
                std::cout << "*******************************Initial queries placed" << std::endl;

                // Compute batches of ISQP events
                std::vector<RequestProcessor::ISQPEventPtr> isqpEvents;

                uint16_t batchIncrement = 1;
                uint16_t addQueryIncrement = 1;
                uint16_t removeQueryIncrement = 1;
                std::vector<QueryId> newPotentialCandidatesForRemoval;
                while (currentIndex < numOfQueries) {

                    //Add query events
                    while (addQueryIncrement <= numOfAddQueries) {
                        auto queryPlan = queryObjects[currentIndex];
                        isqpEvents.emplace_back(RequestProcessor::ISQPAddQueryEvent::create(queryPlan->copy(), placement));
                        newPotentialCandidatesForRemoval.emplace_back(queryPlan->getQueryId());
                        addQueryIncrement++;
                        currentIndex++;
                        if (currentIndex >= numOfQueries) {
                            break;
                        }
                    }

                    //Remove query events
                    while (removeQueryIncrement <= numOfRemoveQueries) {
                        QueryId& queryId = potentialCandidatesForRemoval.rbegin()[removeQueryIncrement - 1];
                        isqpEvents.emplace_back(RequestProcessor::ISQPRemoveQueryEvent::create(queryId));
                        removeQueryIncrement++;
                        currentIndex++;
                    }
                    addQueryIncrement = 1;
                    removeQueryIncrement = 1;
                    potentialCandidatesForRemoval = newPotentialCandidatesForRemoval;
                    newPotentialCandidatesForRemoval.clear();
                }

                std::cout << "*******************************Finished collecting ISQP events " << isqpEvents.size() << std::endl;

                //Compute ISQP batches
                std::vector<std::vector<RequestProcessor::ISQPEventPtr>> isqpBatches;
                uint64_t counter = 0;
                while (counter < isqpEvents.size()) {
                    uint16_t batchSizeCounter = 0;
                    std::vector<RequestProcessor::ISQPEventPtr> tempBatch;
                    while (batchSizeCounter < batchSize) {
                        auto isqpEvent = isqpEvents.at(counter);
                        tempBatch.emplace_back(isqpEvent);
                        batchSizeCounter++;
                        counter++;
                        if (counter >= isqpEvents.size()) {
                            break;
                        }
                    }
                    isqpBatches.emplace_back(tempBatch);
                }

                std::cout << "*******************************Finished preparing ISQP batches " << isqpBatches.size() << std::endl;

                std::vector<std::tuple<uint64_t, uint64_t, RequestProcessor::ISQPRequestResponsePtr>> batchResponses;
                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                uint32_t count = 0;
                // Execute batches of ISQP events
                for (auto const& isqpBatch : isqpBatches) {
                    count++;
                    auto requestEventTime =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    auto response = requestHandlerService->queueISQPRequest(isqpBatch);
                    batchResponses.emplace_back(
                        std::tuple<uint64_t, uint64_t, RequestProcessor::ISQPRequestResponsePtr>(count,
                                                                                                 requestEventTime,
                                                                                                 response));
                }
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                aggregatedBenchmarkOutput << fileName << "," << configNum << "," << placementStrategy << ","
                                          << std::to_string(incrementalPlacement) << "," << placementAmendmentThreadCount << ","
                                          << placementAmendmentMode << "," << numOfAddQueries << "," << numOfRemoveQueries << ","
                                          << batchSize << "," << run << "," << count << "," << startTime << "," << endTime << ","
                                          << (endTime - startTime) << std::endl;

                for (const auto& batchResponse : batchResponses) {
                    uint64_t batchCount = std::get<0>(batchResponse);
                    uint64_t batchEventTime = std::get<1>(batchResponse);
                    RequestProcessor::ISQPRequestResponsePtr response = std::get<2>(batchResponse);
                    detailedBenchmarkOutput << fileName << "," << configNum << "," << placementStrategy << ","
                                            << std::to_string(incrementalPlacement) << "," << placementAmendmentThreadCount << ","
                                            << placementAmendmentMode << "," << batchSize << "," << run << "," << numOfAddQueries
                                            << "," << numOfRemoveQueries << "," << count << "," << batchCount << ","
                                            << batchEventTime << "," << response->processingStartTime << ","
                                            << response->amendmentStartTime << "," << response->processingEndTime << ","
                                            << response->numOfFailedPlacements << "," << response->numOfSQPAffected << std::endl;
                }

                std::cout << "Total SQPs " << nesCoordinator->getGlobalQueryPlan()->getAllSharedQueryPlans().size();
                std::cout << "Finished Run " << run << std::endl;
                nesCoordinator->stopCoordinator(true);
            }

            std::cout << aggregatedBenchmarkOutput.str() << std::endl;
            std::ofstream outAgg("ISQP-Aggregated-Benchmark_" + fileName + ".csv", std::ios::trunc);
            outAgg << aggregatedBenchmarkOutput.str();
            outAgg.close();
            std::ofstream outDetailed("ISQP-Detailed-Benchmark_" + fileName + ".csv", std::ios::trunc);
            outDetailed << detailedBenchmarkOutput.str();
            outDetailed.close();
            std::cout << "---------------------------------------------------------------------------------------------"
                      << std::endl;
            std::cout << "---------------------------------------------------------------------------------------------"
                      << std::endl;
            std::cout << "---------------------------------------------------------------------------------------------"
                      << std::endl;
        }
        // rename filename
        std::cout << "Renaming File: " << file.path().relative_path().string() << std::endl;
        std::filesystem::rename(file.path(), "/" + file.path().relative_path().string() + "_done");
    }
    //Print the benchmark output and same it to the CSV file for further processing
    std::cout << "benchmark finish" << std::endl;
    return 0;
}
