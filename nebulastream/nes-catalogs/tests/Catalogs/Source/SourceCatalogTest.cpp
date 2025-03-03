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
#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

using namespace std;
using namespace NES;
using namespace Configurations;

auto testSchema = Schema::create() -> addField("id", BasicType::UINT32) -> addField("value", BasicType::UINT64);
const std::string defaultLogicalSourceName = "default_logical";

/* - nesTopologyManager ---------------------------------------------------- */
class SourceCatalogTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    }
};

TEST_F(SourceCatalogTest, testAddGetLogSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

    sourceCatalog->addLogicalSource("test_stream", Schema::create());
    SchemaPtr sPtr = sourceCatalog->getSchemaForLogicalSource("test_stream");
    EXPECT_NE(sPtr, nullptr);

    map<std::string, SchemaPtr> allLogicalSource = sourceCatalog->getAllLogicalSource();
    string exp = "id:INTEGER(32 bits) value:INTEGER(64 bits)";
    EXPECT_EQ(allLogicalSource.size(), 2U);

    SchemaPtr testSchema = allLogicalSource["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalSource["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(SourceCatalogTest, testRemovalOfAllPhysicalSourcesByWorkerId) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource("test_stream", Schema::create());
    auto logicalSource = sourceCatalog->getLogicalSource("test_stream");

    auto registerPhysicalSource = [&](const std::string& phyName, WorkerId workerId) {
        sourceCatalog->addPhysicalSource("test_stream",
                                         Catalogs::Source::SourceCatalogEntry::create(
                                             PhysicalSource::create(DefaultSourceType::create("test_stream", phyName)),
                                             logicalSource,
                                             workerId));
    };

    registerPhysicalSource("test_stream_1", WorkerId(1));
    registerPhysicalSource("test_stream_2", WorkerId(1));
    registerPhysicalSource("test_stream_3", WorkerId(1));
    registerPhysicalSource("test_stream_4", WorkerId(1));
    registerPhysicalSource("test_stream_5", WorkerId(3));
    registerPhysicalSource("test_stream_6", WorkerId(2));
    registerPhysicalSource("test_stream_7", WorkerId(2));

    EXPECT_THAT(sourceCatalog->getPhysicalSources("test_stream"), ::testing::SizeIs(7));
    EXPECT_EQ(sourceCatalog->removeAllPhysicalSourcesByWorker(WorkerId(1)), 4);
    EXPECT_THAT(sourceCatalog->getPhysicalSources("test_stream"), ::testing::SizeIs(3));
    EXPECT_EQ(sourceCatalog->removeAllPhysicalSourcesByWorker(WorkerId(2)), 2);
    EXPECT_THAT(sourceCatalog->getPhysicalSources("test_stream"), ::testing::SizeIs(1));
    EXPECT_EQ(sourceCatalog->getPhysicalSources("test_stream")[0]->getTopologyNodeId(), WorkerId(3));
    EXPECT_EQ(sourceCatalog->getPhysicalSources("test_stream")[0]->getPhysicalSource()->getPhysicalSourceName(), "test_stream_5");
    EXPECT_EQ(sourceCatalog->removeAllPhysicalSourcesByWorker(WorkerId(3)), 1);
    EXPECT_THAT(sourceCatalog->getPhysicalSources("test_stream"), ::testing::IsEmpty());
}

TEST_F(SourceCatalogTest, testAddRemoveLogSource) {
    sourceCatalog->addLogicalSource("test_stream", Schema::create());

    EXPECT_TRUE(sourceCatalog->removeLogicalSource("test_stream"));

    EXPECT_THROW(sourceCatalog->getSchemaForLogicalSource("test_stream"), Exceptions::LogicalSourceNotFoundException);

    string exp = "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream "
                 "name=test_stream schema:\n\n";

    map<std::string, SchemaPtr> allLogicalSource = sourceCatalog->getAllLogicalSource();

    EXPECT_EQ(1ul, allLogicalSource.size());
    EXPECT_FALSE(sourceCatalog->removeLogicalSource("test_stream22"));
}

TEST_F(SourceCatalogTest, testGetNotExistingKey) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    EXPECT_THROW(sourceCatalog->getSchemaForLogicalSource("test_stream22"), Exceptions::LogicalSourceNotFoundException);
}

TEST_F(SourceCatalogTest, testAddGetPhysicalSource) {

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(WorkerId(1), "localhost", 4000, 4002, 4, properties);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create(logicalSource->getLogicalSourceName(), "physicalSource");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
}

//TODO: add test for a second physical source add

TEST_F(SourceCatalogTest, testAddRemovePhysicalSource) {

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(WorkerId(1), "localhost", 4000, 4002, 4, properties);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create(logicalSource->getLogicalSourceName(), "physicalSource");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    EXPECT_TRUE(sourceCatalog->removePhysicalSource(physicalSource->getLogicalSourceName(),
                                                    physicalSource->getPhysicalSourceName(),
                                                    physicalNode->getId()));
    NES_INFO("{}", sourceCatalog->getPhysicalSourceAndSchemaAsString());
}

TEST_F(SourceCatalogTest, testAddPhysicalForNotExistingLogicalSource) {
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(WorkerId(1), "localhost", 4000, 4002, 4, properties);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create(logicalSource->getLogicalSourceName(), "physicalSource");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
}

//new from service
TEST_F(SourceCatalogTest, testGetAllLogicalSource) {

    const map<std::string, std::string>& allLogicalSource = sourceCatalog->getAllLogicalSourceAsString();
    EXPECT_EQ(allLogicalSource.size(), 1U);
    for (auto const& [key, value] : allLogicalSource) {
        bool cmp = key != defaultLogicalSourceName;
        EXPECT_EQ(cmp, false);
    }
}

TEST_F(SourceCatalogTest, testAddLogicalSourceFromString) {
    sourceCatalog->addLogicalSource("test", testSchema);
    const map<std::string, std::string>& allLogicalSource = sourceCatalog->getAllLogicalSourceAsString();
    EXPECT_EQ(allLogicalSource.size(), 2U);
}

TEST_F(SourceCatalogTest, testGetPhysicalSourceForLogicalSource) {
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(WorkerId(1), "localhost", 4000, 4002, 4, properties);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create(logicalSource->getLogicalSourceName(), "physicalSource");
    auto physicalSource = PhysicalSource::create(defaultSourceType);
    auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    const vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSources =
        sourceCatalog->getPhysicalSources(logicalSource->getLogicalSourceName());
    EXPECT_EQ(allPhysicalSources.size(), 1U);
}

TEST_F(SourceCatalogTest, testDeleteLogicalSource) {
    bool success = sourceCatalog->removeLogicalSource(defaultLogicalSourceName);
    EXPECT_TRUE(success);
}

TEST_F(SourceCatalogTest, testUpdateLogicalSourceWithInvalidSourceName) {
    std::string logicalSourceName = "test";
    auto newSchema = Schema::create()->addField("id", BasicType::UINT32);
    bool success = sourceCatalog->updateLogicalSource(logicalSourceName, newSchema);
    EXPECT_FALSE(success);
}

TEST_F(SourceCatalogTest, testUpdateLogicalSource) {
    std::string logicalSourceName = "test";
    bool success = sourceCatalog->addLogicalSource(logicalSourceName, testSchema);
    EXPECT_TRUE(success);

    auto newSchema = Schema::create()->addField("id", BasicType::UINT32);
    success = sourceCatalog->updateLogicalSource(logicalSourceName, newSchema);
    EXPECT_TRUE(success);
}
