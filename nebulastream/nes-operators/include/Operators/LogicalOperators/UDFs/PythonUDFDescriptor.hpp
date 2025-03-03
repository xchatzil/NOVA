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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_PYTHONUDFDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_PYTHONUDFDESCRIPTOR_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <memory>
#include <string>

namespace NES::Catalogs::UDF {

class PythonUDFDescriptor;
using PythonUDFDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;

class PythonUDFDescriptor : public UDFDescriptor {
  public:
    PythonUDFDescriptor(const std::string& functionName,
                        const std::string& functionString,
                        const SchemaPtr& inputSchema,
                        const SchemaPtr& outputSchema);

    static PythonUDFDescriptorPtr create(const std::string& functionName,
                                         const std::string& functionString,
                                         const SchemaPtr inputSchema,
                                         const SchemaPtr outputSchema) {
        return std::make_shared<PythonUDFDescriptor>(functionName, functionString, inputSchema, outputSchema);
    }

    /**
     * @brief Return the fully-qualified class name of the class implementing the UDF.
     * @return Fully-qualified class name of the class implementing the UDF.
     */
    const std::string& getFunctionString() const { return functionString; }

    /**
     * @brief Generates the infer string signature required for the logical operator
     * @return the infer string signature stream
     */
    std::stringstream generateInferStringSignature() override;

    /**
     * Compare to Python UDF descriptors.
     *
     * @param other The other PythonUDFDescriptor in the comparison.
     * @return True, if both PythonUdfDescriptors are the same, i.e., same UDF class and method name,
     * same serialized instance (state), and same byte code list; False, otherwise.
     */
    bool operator==(const PythonUDFDescriptor& other) const;

  private:
    const std::string functionString;
};
}// namespace NES::Catalogs::UDF
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_PYTHONUDFDESCRIPTOR_HPP_
