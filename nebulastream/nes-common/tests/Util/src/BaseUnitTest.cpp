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
#include <BaseUnitTest.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
namespace Exceptions {
extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
}// namespace Exceptions

namespace Testing {

void BaseUnitTest::SetUp() {
    testing::Test::SetUp();
    Exceptions::installGlobalErrorListener(self = std::shared_ptr<Exceptions::ErrorListener>(this, Deleter()));
    startWaitingThread(typeid(*this).name());
}

void BaseUnitTest::TearDown() {
    testing::Test::TearDown();
    completeTest();
    Logger::getInstance()->forceFlush();
    Exceptions::removeGlobalErrorListener(self);
    self.reset();
}

void BaseUnitTest::onFatalError(int signalNumber, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalError: signal [{}] error [{}] (enable NES_DEBUG to view stacktrace)", signalNumber, strerror(errno));
    } else {
        NES_ERROR("onFatalError: signal [{}] error [{}] callstack [{}]", signalNumber, strerror(errno), callstack);
    }
    failTest();
    FAIL();
}

void BaseUnitTest::onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalException: exception=[{}] (enable NES_DEBUG to view stacktrace)", exception->what());
    } else {
        NES_ERROR("onFatalException: exception=[{}] callstack=\n{}", exception->what(), callstack);
    }
    failTest();
    FAIL();
}

namespace detail {

TestWaitingHelper::TestWaitingHelper() { testCompletion = std::make_shared<std::promise<bool>>(); }

void TestWaitingHelper::failTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(false);
        waitThread->join();
        waitThread.reset();
    }
}

void TestWaitingHelper::completeTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(true);
        waitThread->join();
        waitThread.reset();
    }
}

void TestWaitingHelper::startWaitingThread(std::string testName) {
    waitThread = std::make_unique<std::thread>([this, testName = std::move(testName)]() mutable {
        auto future = testCompletion->get_future();
        switch (future.wait_for(std::chrono::minutes(WAIT_TIME_SETUP))) {
            case std::future_status::ready: {
                try {
                    auto res = future.get();
                    if (!res) {
                        NES_FATAL_ERROR("Got error in test [{}]", testName);
                    }
                } catch (std::exception const& exception) {
                    NES_FATAL_ERROR("Got exception in test [{}]: {}", testName, exception.what());
                    FAIL();
                }
                break;
            }
            case std::future_status::timeout:
            case std::future_status::deferred: {
                NES_ERROR("Cannot terminate test [{}] within deadline", testName);
                FAIL();
            }
        }
    });
}

TestSourceNameHelper::TestSourceNameHelper() { srcCnt = 1; }

std::string TestSourceNameHelper::operator*() {
    std::ostringstream oss;
    oss << "source" << srcCnt++;
    return oss.str();
}

}// namespace detail
}// namespace Testing
}// namespace NES
