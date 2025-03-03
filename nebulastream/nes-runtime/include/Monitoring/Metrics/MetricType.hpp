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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_

#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES::Monitoring {

/**
* @brief The metric types of NES represented in the subdirectories of Metrics/..
*/
enum class MetricType : uint8_t {
    CpuMetric,
    DiskMetric,
    MemoryMetric,
    NetworkMetric,
    RuntimeMetric,
    RegistrationMetric,
    WrappedCpuMetrics,
    WrappedNetworkMetrics,
    UnknownMetric
};

std::string toString(MetricType metricType);

}// namespace NES::Monitoring

#endif// NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_
