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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>

namespace NES {

class DefaultSourceType;
using DefaultSourceTypePtr = std::shared_ptr<DefaultSourceType>;

/**
 * @brief Configuration object for default source config
 * A simple source with default data created inside NES, useful for testing
 */
class DefaultSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypePtr create(const std::string& logicalSourceName,
                                       const std::string& physicalSourceName,
                                       std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypePtr
    create(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig);

    /**
     * @brief create defaultSourceConfig with default values
     * @return defaultSourceConfig with default values
     */
    static DefaultSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName);

    const Configurations::IntConfigOption& getNumberOfBuffersToProduce() const;

    const Configurations::IntConfigOption& getSourceGatheringInterval() const;

    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    void setSourceGatheringInterval(uint32_t sourceGatheringInterval);

    /**
     * @brief Get gathering mode
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<GatheringMode>> getGatheringMode() const;

    /**
     * @brief Set gathering mode
     */
    void setGatheringMode(std::string inputGatheringMode);

    /**
     * @brief Sets the gathering mode given as GatheringMode
     * @param inputGatheringMode
     */
    void setGatheringMode(GatheringMode inputGatheringMode);

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

  private:
    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for physicalSources
     * @param sourceConfigMap: the source configuration map
     */
    explicit DefaultSourceType(const std::string& logicalSourceName,
                               const std::string& physicalSourceName,
                               std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for physicalSources
     * @param sourceTypeConfig: the configuration for yaml
     */
    explicit DefaultSourceType(const std::string& logicalSourceName,
                               const std::string& physicalSourceName,
                               Yaml::Node sourceTypeConfig);

    /**
     * @brief constructor to create a new Default source config object initialized with default values
     */
    DefaultSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName);

    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption sourceGatheringInterval;

    /**
     * @brief the gathering mode of the sampling function.
     */
    Configurations::GatheringModeConfigOption gatheringMode;
};
}// namespace NES
#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_
