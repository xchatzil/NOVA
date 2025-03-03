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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_MONITORINGSOURCETYPE_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_MONITORINGSOURCETYPE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/MetricCollectorType.hpp>
#include <chrono>

namespace NES {

class MonitoringSourceType;
using MonitoringSourceTypePtr = std::shared_ptr<MonitoringSourceType>;

/**
 * @brief Configuration object for monitoring source config
 */
class MonitoringSourceType : public PhysicalSourceType {
  public:
    ~MonitoringSourceType() noexcept override = default;

    /**
     * @brief create a MonitoringSourceTypePtr object
     * @return MonitoringSourceTypePtr
     */
    static MonitoringSourceTypePtr create(const std::string& logicalSourceName,
                                          const std::string& physicalSourceName,
                                          Monitoring::MetricCollectorType metricCollectorType,
                                          std::chrono::milliseconds waitTimeInMs);

    /**
     * @brief create a MonitoringSourceTypePtr object
     * @return MonitoringSourceTypePtr
     */
    static MonitoringSourceTypePtr create(const std::string& logicalSourceName,
                                          const std::string& physicalSourceName,
                                          Monitoring::MetricCollectorType metricCollectorType);

    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other mqttSourceType ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief gets a chrono object with the wait time
     */
    [[nodiscard]] std::chrono::milliseconds getWaitTime() const;

    /**
     * @brief set the value for wait time with the appropriate data format
     */
    void setWaitTime(std::chrono::milliseconds waitTime);

    /**
     * @brief gets a int object representing the enum of metric collector type
     */
    [[nodiscard]] Monitoring::MetricCollectorType getMetricCollectorType() const;

    /**
     * @brief set the value for collector type with the appropriate data format
     */
    void setMetricCollectorType(Monitoring::MetricCollectorType metricCollectorType);

  private:
    /**
     * @brief constructor to create a new source type with defaults.
     */
    MonitoringSourceType(const std::string& logicalSourceName,
                         const std::string& physicalSourceName,
                         Monitoring::MetricCollectorType metricCollectorType,
                         std::chrono::milliseconds waitTime);
    Monitoring::MetricCollectorType metricCollectorType;
    std::chrono::milliseconds waitTime;
    static constexpr std::chrono::milliseconds DEFAULT_WAIT_TIME = std::chrono::milliseconds(1000);
};

}// namespace NES

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_MONITORINGSOURCETYPE_HPP_
