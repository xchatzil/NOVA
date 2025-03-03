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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_ZMQSERVER_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_ZMQSERVER_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Runtime/BufferManager.hpp>
#include <atomic>
#include <future>
#include <memory>
#include <thread>
#include <zmq.hpp>

namespace NES {
class ThreadBarrier;
namespace Network {

/**
 * @brief ZMQ server on hostname:port with numNetworkThreads i/o threads and a set of callbacks in
 * exchangeProtocol.
 * This class is not copyable.
 */
class ZmqServer {
  private:
    static constexpr const char* dispatcherPipe = "inproc://dispatcher";

  public:
    /**
     * Create a ZMQ server on hostname:port with numNetworkThreads i/o threads and a set of callbacks in
     * exchangeProtocol
     * @param hostname
     * @param port
     * @param numNetworkThreads
     * @param exchangeProtocol
     */
    explicit ZmqServer(std::string hostname,
                       uint16_t requestedPort,
                       uint16_t numNetworkThreads,
                       ExchangeProtocol& exchangeProtocol,
                       Runtime::BufferManagerPtr bufferManager);

    ~ZmqServer();

    /**
     * Start the server. It throws exceptions if the starting fails.
     */
    bool start();

    /**
    * Stop the server. It throws exceptions if the stopping fails.
    */
    bool stop();

    /**
     * Get the global zmq context
     * @return
     */
    std::shared_ptr<zmq::context_t> getContext() { return zmqContext; }

    /**
     * Checks if the server is running
     * @return true if running
     */
    [[nodiscard]] bool isServerRunning() const { return isRunning; }

    /**
     * Returns the current server port
     * @return the current server port
     */
    [[nodiscard]] uint16_t getServerPort() const { return currentPort.load(); }
    /**
     * Returns the hostname
     * @return the current hostname
     */
    [[nodiscard]] std::string getHostname() const { return hostname; }
    /**
     * Returns the hostname
     * @return the current hostname
     */
    [[nodiscard]] uint16_t getNumOfThreads() const { return numNetworkThreads; }
    /**
     * Returns the hostname
     * @return the current hostname
     */
    [[nodiscard]] uint16_t getRequestedPort() const { return requestedPort; }
    /**
     * @brief Retrieves the current server socket information
     * @param hostname the hostname in use
     * @param port the port in use
     */
    void getServerSocketInfo(std::string& hostname, uint16_t& port);

  private:
    /**
    * @brief Remove copy constructor to make this class not copyable
    */
    ZmqServer(const ZmqServer&) = delete;

    /**
    * @brief Remove assignment to make this class not copyable
    */
    ZmqServer& operator=(const ZmqServer&) = delete;

    /**
     * @brief the receiving thread where clients send their messages to the server, here messages are forwarded to the
     * handlerEventLoop by the proxy
     * @param numHandlerThreads number of HandlerThreads
     * @param startPromise the promise that is passed to the thread
     */
    void routerLoop(uint16_t numHandlerThreads, const std::shared_ptr<std::promise<bool>>& startPromise);

    /**
     * @brief handler thread where threads are passed from the frontend loop
     * @param barrier the threadBarrier to enable synchronization
     * @param threadId the id of the thread running the handler event loop
     */
    void messageHandlerEventLoop(const std::shared_ptr<ThreadBarrier>& barrier, int threadId);

    const std::string hostname;
    /// this is the port from the configuration: can be 0
    const uint16_t requestedPort;
    /// this is the port that the server actually binds to: cant be 0
    std::atomic<uint16_t> currentPort;
    const uint16_t numNetworkThreads;

    std::shared_ptr<zmq::context_t> zmqContext;
    std::unique_ptr<std::thread> routerThread;
    std::vector<std::unique_ptr<std::thread>> handlerThreads;

    std::atomic_bool isRunning;
    std::atomic_bool keepRunning;

    ExchangeProtocol& exchangeProtocol;
    Runtime::BufferManagerPtr bufferManager;

    /**
     * @brief error management done using 3 values
     *   true: gracefully closed
     *   false: not gracefully closed
     *   exception: error
     */
    std::promise<bool> errorPromise;
};

}// namespace Network
}// namespace NES

namespace fmt {
template<>
struct formatter<NES::Network::ZmqServer> : formatter<std::string> {
    auto format(const NES::Network::ZmqServer& zmq, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "Hostname: {} requested Port:{} number of network threads: {}",
                              zmq.getHostname(),
                              zmq.getRequestedPort(),
                              zmq.getNumOfThreads());
    }
};
}// namespace fmt

#endif// NES_RUNTIME_INCLUDE_NETWORK_ZMQSERVER_HPP_
