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

#include <BaseIntegrationTest.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/File.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Compiler {

static constexpr auto TEST_FILE_BODY = "/*\n"
                                       "    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)\n"
                                       "\n"
                                       "    Licensed under the Apache License, Version 2.0 (the \"License\");\n"
                                       "    you may not use this file except in compliance with the License.\n"
                                       "    You may obtain a copy of the License at\n"
                                       "\n"
                                       "        https://www.apache.org/licenses/LICENSE-2.0\n"
                                       "\n"
                                       "    Unless required by applicable law or agreed to in writing, software\n"
                                       "    distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                                       "    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                                       "    See the License for the specific language governing permissions and\n"
                                       "    limitations under the License.\n"
                                       "*/\n"
                                       "int mul(int x, int y) { return x * y; }";

class JITCompilerTest : public Testing::BaseUnitTest {
  public:
    uint64_t waitForCompilation = 10;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JITCompilerTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup JITCompilerTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        std::cout << "Setup JITCompilerTest test case." << std::endl;
        auto cppCompiler = CPPCompiler::create();
        auto compilerBuilder = JITCompilerBuilder();
        compilerBuilder.registerLanguageCompiler(cppCompiler);
        compiler = compilerBuilder.build();
    }

    std::shared_ptr<JITCompiler> compiler;
};

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(JITCompilerTest, compileCppCode) {
    auto sourceCode = std::make_unique<SourceCode>(Language::CPP, TEST_FILE_BODY);
    auto request = CompilationRequest::create(std::move(sourceCode), "test_1", false, false, true, true);

    auto result = compiler->compile(std::move(request));
    result.wait_for(std::chrono::seconds(waitForCompilation));

    auto compilationResult = result.get();
    auto mulFunction = compilationResult.getDynamicObject()->getInvocableMember<int (*)(int, int)>("_Z3mulii");
    auto mulRes = mulFunction(10, 10);
    ASSERT_EQ(mulRes, 100);
    NES_DEBUG("CompilationTime:{}", compilationResult.getCompilationTime());
}

}// namespace NES::Compiler
