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
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {

DiskCollector::DiskCollector()
    : MetricCollector(), resourceReader(SystemResourcesReaderFactory::getSystemResourcesReader()),
      schema(DiskMetrics::getSchema("")) {
    NES_INFO("DiskCollector: Init DiskCollector with schema {}", schema->toString());
}

MetricCollectorType DiskCollector::getType() { return MetricCollectorType::DISK_COLLECTOR; }

bool DiskCollector::fillBuffer(Runtime::TupleBuffer& tupleBuffer) {
    try {
        DiskMetrics measuredVal = resourceReader->readDiskStats();
        measuredVal.nodeId = getWorkerId();
        writeToBuffer(measuredVal, tupleBuffer, 0);
    } catch (const std::exception& ex) {
        NES_ERROR("DiskCollector: Error while collecting metrics {}", ex.what());
        return false;
    }
    return true;
}

SchemaPtr DiskCollector::getSchema() { return schema; }

const MetricPtr DiskCollector::readMetric() const {
    DiskMetrics metrics = resourceReader->readDiskStats();
    metrics.nodeId = getWorkerId();
    return std::make_shared<Metric>(std::move(metrics), MetricType::DiskMetric);
}

}// namespace NES::Monitoring
