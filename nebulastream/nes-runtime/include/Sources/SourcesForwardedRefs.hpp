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
#ifndef NES_RUNTIME_INCLUDE_SOURCES_SOURCESFORWARDEDREFS_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_SOURCESFORWARDEDREFS_HPP_

#include <memory>
namespace NES {

class DataSource;
using DataSourcePtr = std::shared_ptr<DataSource>;

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class MetricGroup;
using MetricGroupPtr = std::shared_ptr<MetricGroup>;

class MetricCatalog;
using MetricCatalogPtr = std::shared_ptr<MetricCatalog>;

namespace Runtime {

class TupleBuffer;

namespace detail {
class MemorySegment;
}// namespace detail
}// namespace Runtime

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_SOURCESFORWARDEDREFS_HPP_
