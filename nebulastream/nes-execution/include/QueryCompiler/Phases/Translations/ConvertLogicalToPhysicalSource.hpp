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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sources/DataSource.hpp>

namespace NES {

/**
 * @brief This class is responsible for creating logical source descriptor to physical source.
 */
class ConvertLogicalToPhysicalSource {

  public:
    /**
     * @brief This method produces corresponding physical source for an input logical source descriptor
     * @param operator id: the operator id
     * @param sourceDescriptor : the logical source descriptor
     * @return Data source pointer for the physical source
     */
    static DataSourcePtr createDataSource(OperatorId operatorId,
                                          OriginId originId,
                                          StatisticId statisticId,
                                          const SourceDescriptorPtr& sourceDescriptor,
                                          const Runtime::NodeEnginePtr& nodeEngine,
                                          size_t numSourceLocalBuffers,
                                          const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors =
                                              std::vector<Runtime::Execution::SuccessorExecutablePipeline>());

  private:
    ConvertLogicalToPhysicalSource() = default;
};

}// namespace NES

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
