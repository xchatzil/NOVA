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
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <Sinks/DataSinkPlugin.hpp>
#include <utility>

namespace NES::QueryCompilation {

DataSinkProviderPtr DataSinkProvider::create() { return std::make_shared<DataSinkProvider>(); }

DataSinkPtr DataSinkProvider::lower(OperatorId sinkId,
                                    SinkDescriptorPtr sinkDescriptor,
                                    SchemaPtr schema,
                                    Runtime::NodeEnginePtr nodeEngine,
                                    const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                                    size_t numOfProducers) {
    for (const auto& plugin : SinkPluginRegistry::getPlugins()) {
        auto dataSink = plugin->createDataSink(sinkId, sinkDescriptor, schema, nodeEngine, querySubPlan, numOfProducers);
        if (dataSink.has_value()) {
            return dataSink.value();
        }
    }
    return ConvertLogicalToPhysicalSink::createDataSink(sinkId,
                                                        std::move(sinkDescriptor),
                                                        std::move(schema),
                                                        std::move(nodeEngine),
                                                        querySubPlan,
                                                        numOfProducers);
}

}// namespace NES::QueryCompilation
