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
#ifndef NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_

#include <Compiler/CompilerFlags.hpp>

namespace NES::Compiler {

/**
 * @brief Represents compilation flags available to the C++ compiler.
 */
class CPPCompilerFlags : public CompilerFlags {
  public:
    // sets the cpp language version for the code
#if defined(__APPLE__)
    inline static const std::string CXX_VERSION = "-std=c++20 -stdlib=libc++";
#else
    inline static const std::string CXX_VERSION = "-std=c++20 -stdlib=libstdc++";
#endif
    // disables trigraphs
    inline static const std::string NO_TRIGRAPHS = "-fno-trigraphs";
    //Position Independent Code means that the generated machine code is not dependent on being located at a specific address in order to work.
    inline static const std::string FPIC = "-fpic";
    // Turn warnings into errors.
    inline static const std::string WERROR = "-Werror";
    // warning: equality comparison with extraneous parentheses
    inline static const std::string WPARENTHESES_EQUALITY = "-Wparentheses-equality";
    // Create a shared library.
    inline static const std::string SHARED = "-shared";

    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_FATAL_ERROR_LEVEL=1";
    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_TRACE_LEVEL=1";

    inline static const std::string ALL_OPTIMIZATIONS = "-O3";
    inline static const std::string TUNE = "-mtune=native";
    inline static const std::string ARCH = "-march=native";
    // ARM suggests to use cpu type instead of tune/arch
    inline static const std::string CPU = "-mcpu=native";
    // LLVM defines specific to this cpu type, for now a14 == m1
    // https://github.com/llvm/llvm-project/blob/llvmorg-13.0.0/llvm/include/llvm/Support/AArch64TargetParser.def
    // See issue: https://github.com/nebulastream/nebulastream/issues/2248
    inline static const std::string M1_CPU = "-mcpu=apple-a14";
    inline static const std::string GENERATE_DEBUG_SYMBOLS = "-g";
    // Enables tracing for compilation time with chrome::tracing
    inline static const std::string TRACE_COMPILATION_TIME = "-ftime-trace";

    // Vector extensions
    inline static const std::string SSE_4_1 = "-msse4.1";
    inline static const std::string SSE_4_2 = "-msse4.2";
    inline static const std::string AVX = "-mavx";
    inline static const std::string AVX2 = "-mavx2";

    CPPCompilerFlags() = default;

    void addDefaultCompilerFlags();
    void addSharedLibraryFlag();

    void enableOptimizationFlags();
    void enableDebugFlags();
    void enableProfilingFlags();
};

}// namespace NES::Compiler

#endif// NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILERFLAGS_HPP_
