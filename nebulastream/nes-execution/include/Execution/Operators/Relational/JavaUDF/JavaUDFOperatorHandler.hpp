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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This handler stores states of a MapJavaUDF operator during its execution.
 */
class JavaUDFOperatorHandler : public OperatorHandler {
  public:
    /**
     * @brief This creates a JavaUDFOperatorHandler
     * @param className The java class name containing the java udf
     * @param methodName The java method name of the java udf
     * @param inputClassName The class name of the input type of the udf
     * @param outputClassName The class name of the output type of the udf
     * @param byteCodeList The byteCode containing serialized java objects to load into jvm
     * @param serializedInstance The serialized instance of the java java class
     * @param udfInputSchema The input schema of the Java UDF
     * @param udfOutputSchema The output schema of the Java UDF
     * @param javaPath Optional: path to jar files to load classes from into JVM
     */
    explicit JavaUDFOperatorHandler(const std::string& className,
                                    const std::string& methodName,
                                    const std::string& inputClassName,
                                    const std::string& outputClassName,
                                    const jni::JavaUDFByteCodeList& byteCodeList,
                                    const jni::JavaSerializedInstance& serializedInstance,
                                    SchemaPtr udfInputSchema,
                                    SchemaPtr udfOutputSchema,
                                    const std::optional<std::string>& javaPath);

    /**
     * @brief Initializes the operator handler and the jvm if required.
     */
    void setup();

    /**
     * @brief Convert a Java class name from Java notation (e.g., java.lang.Object), to JNI notation (e.g., java/lang/Object).
     * @param javaClassName The class name in Java notation.
     * @return The class name in JNI notation.
     */
    static const std::string convertToJNIName(const std::string& javaClassName);

    /**
     * @brief This method returns the class name of the java udf
     * @return std::string class name
     */
    const std::string& getClassName() const;

    /**
     * @brief This method returns the method name of the java udf
     * @return std::string method name
     */
    const std::string& getMethodName() const;

    /**
     * @brief This method returns the class name of the input class name of the java udf
     * @return std::string input class name
     */
    const std::string& getInputClassName() const;

    /**
     * @brief This method returns the class name of the input class name of the Java UDF in JNI notation.
     * @return std::string input class name in JNI notation.
     */
    const std::string& getInputClassJNIName() const;

    /**
     * @brief This method returns the class name of the output class name of the Java UDF in JNI notation.
     * @return std::string output class name
     */
    const std::string& getOutputClassJNIName() const;

    /**
     * @brief This method returns the byte code list of the java udf
     * @return std::unordered_map<std::string, std::vector<char>> byte code list
     */
    const jni::JavaUDFByteCodeList& getByteCodeList() const;

    /**
     * @brief This method returns the serialized instance of the java udf
     * @return std::vector<char> serialized instance
     */
    const jni::JavaSerializedInstance& getSerializedInstance() const;

    /**
     * @brief This method returns the input schema of the Java UDF.
     * @return SchemaPtr Java UDF input schema
     */
    const SchemaPtr& getUdfInputSchema() const;

    /**
     * @brief This method returns the output schema of the Java UDF.
     * @return SchemaPtr Java UDF output schema
     */
    const SchemaPtr& getUdfOutputSchema() const;

    /**
     * @brief This method returns the java udf object state
     * @return jobject java udf object
     */
    jni::jobject getUdfInstance() const { return udfInstance; }

    /**
     * @brief This method returns the java udf method id
     * @return jmethodID java udf method id
     */
    jni::jmethodID getUDFMethodId() const;

    /**
     * @brief Find a class inside the custom class loader associated with the UDF.
     * @param className The name of the class in Java notation.
     */
    jni::jclass loadClass(const std::string_view& className) const;

    void start(PipelineExecutionContextPtr, uint32_t) override;
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override;

    ~JavaUDFOperatorHandler();

  private:
    /** @brief Setup a custom class loader for this UDF. */
    void setupClassLoader();
    /** @brief Inject classes of this UDF into the JVM. */
    void injectClassesIntoClassLoader() const;
    /** @brief Deserialize the UDF instance. */
    void deserializeInstance();

    const std::string className;
    const std::string classJNIName;
    const std::string methodName;
    const std::string inputClassName;
    const std::string inputClassJNIName;
    const std::string outputClassName;
    const std::string outputClassJNIName;
    const jni::JavaUDFByteCodeList byteCodeList;
    const jni::JavaSerializedInstance serializedInstance;
    const SchemaPtr udfInputSchema;
    const SchemaPtr udfOutputSchema;
    const SchemaPtr operatorInputSchema;
    const SchemaPtr operatorOutputSchema;
    const std::optional<std::string> javaPath;
    jni::jmethodID udfMethodId;
    jni::jobject udfInstance = nullptr;
    jni::jobject classLoader = nullptr;
    jni::jmethodID injectClassMethod;
    jni::jmethodID loadClassMethod;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFOPERATORHANDLER_HPP_
