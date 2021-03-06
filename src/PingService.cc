/* Copyright (c) 2011-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include<fstream>
#include "Common.h"
#include "BackupService.h"
#include "BasicTransport.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "MasterService.h"
#include "RawMetrics.h"
#include "ShortMacros.h"
#include "PerfStats.h"
#include "PingClient.h"
#include "PingService.h"
#include "ServerList.h"
#include "TimeTrace.h"
#include "CacheTrace.h"

namespace RAMCloud {

/**
 * Construct a PingService.
 *
 * \param context
 *      Overall information about the RAMCloud server. The caller is assumed
 *      to have associated a serverList with this context; if not, this service
 *      will not return a valid ServerList version in response to pings.
 *      The new service will be registered in this context.
 */
PingService::PingService(Context* context)
    : context(context)
    , ignoreKill(false)
    , returnUnknownId(false)
{
    context->services[WireFormat::PING_SERVICE] = this;
}

PingService::~PingService()
{
    context->services[WireFormat::PING_SERVICE] = NULL;
}

/**
 * Top-level service method to handle the GET_METRICS request.
 *
 * \copydetails Service::ping
 */
void
PingService::getMetrics(const WireFormat::GetMetrics::Request* reqHdr,
             WireFormat::GetMetrics::Response* respHdr,
             Rpc* rpc)
{
    string serialized;
    metrics->serialize(serialized);
    respHdr->messageLength = downCast<uint32_t>(serialized.length());
    rpc->replyPayload->appendCopy(serialized.c_str(), respHdr->messageLength);
}

/**
 * Top-level service method to handle the GET_SERVER_ID request.
 *
 * \copydetails Service::ping
 */
void
PingService::getServerId(const WireFormat::GetServerId::Request* reqHdr,
             WireFormat::GetServerId::Response* respHdr,
             Rpc* rpc)
{
    if (returnUnknownId) {
        returnUnknownId = false;
        respHdr->serverId = ServerId().getId();
    } else {
        respHdr->serverId = serverId.getId();
        if (!serverId.isValid()) {
            RAMCLOUD_LOG(NOTICE, "Returning invalid server id");
        }
    }
}

/**
 * Top-level service method to handle the PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::ping(const WireFormat::Ping::Request* reqHdr,
             WireFormat::Ping::Response* respHdr,
             Rpc* rpc)
{
    uint64_t ticks = 0;
    CycleCounter<> counter(&ticks);

    string callerId = ServerId(reqHdr->callerId).toString();

    ServerId serverId(reqHdr->callerId);
    if (serverId.isValid()) {
        // Careful, turning this into a real log message causes spurious
        // ping timeouts.
        TEST_LOG("Received ping request from server %s",
                 serverId.toString().c_str());
        if (!context->serverList->isUp(serverId)) {
            LOG(WARNING, "Received ping from server not in cluster: %s",
                    serverId.toString().c_str());
            respHdr->common.status = STATUS_CALLER_NOT_IN_CLUSTER;
        }
    }

    counter.stop();
    double ms = Cycles::toSeconds(ticks) * 1000;
    if (ms > 10) {
        LOG(WARNING, "Slow responding to ping request from server %s; "
            "took %.2f ms", callerId.c_str(), ms);
    }
}

/**
 * Top-level service method to handle the PROXY_PING request.
 *
 * \copydetails Service::ping
 */
void
PingService::proxyPing(const WireFormat::ProxyPing::Request* reqHdr,
             WireFormat::ProxyPing::Response* respHdr,
             Rpc* rpc)
{
    uint64_t start = Cycles::rdtsc();
    PingRpc pingRpc(context, ServerId(reqHdr->serverId));
    respHdr->replyNanoseconds = ~0UL;
    if (pingRpc.wait(reqHdr->timeoutNanoseconds)) {
        respHdr->replyNanoseconds = Cycles::toNanoseconds(
                Cycles::rdtsc() - start);
    }
}

/**
 * Top-level service method to handle the SERVER_CONTROL request.
 *
 * Based on the ControlOp field in the RPC header, this method decides
 * a proper control action to be taken. Any new ControlOp and consequent
 * actions and method calls should be added as a new case item in the
 * switch-case statement below.
 *
 * \copydetails Service::ping
 */
void
PingService::serverControl(const WireFormat::ServerControl::Request* reqHdr,
                           WireFormat::ServerControl::Response* respHdr,
                           Rpc* rpc)
{
    respHdr->serverId = serverId.getId();

    // Perform necessary checks based on RpcType
    switch (reqHdr->type) {
        case WireFormat::ServerControl::OBJECT:
        {
            // We should only get this operation if we own a
            // particular object.
            // Check if there is actually a Master Service running.
            if (context->getMasterService() == NULL) {
                respHdr->common.status = STATUS_UNKNOWN_TABLET;
                return;
            }

            // Check if the RPC has reached the server owning the target object.
            const void* stringKey = rpc->requestPayload->getRange(
                                        sizeof32(*reqHdr), reqHdr->keyLength);
            if (stringKey == NULL) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                return;
            }

            Key key(reqHdr->tableId, stringKey, reqHdr->keyLength);
            TabletManager::Tablet tablet;

            if (!context->getMasterService()->tabletManager.getTablet(
                    key, &tablet) || tablet.state != TabletManager::NORMAL) {
                respHdr->common.status = STATUS_UNKNOWN_TABLET;
                return;
            }
            break;
        }
        case WireFormat::ServerControl::INDEX:
        {
            // We should only get this operation if we own a
            // particular indexlet.
            // Check if there is actually a Master Service running.
            if (context->getMasterService() == NULL) {
                respHdr->common.status = STATUS_UNKNOWN_INDEXLET;
                return;
            }

            // Check if the RPC has reached the server owning the target index.
            const void* stringKey = rpc->requestPayload->getRange(
                                        sizeof32(*reqHdr), reqHdr->keyLength);
            if (stringKey == NULL) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                return;
            }

            if (!context->getMasterService()->indexletManager.hasIndexlet(
                    reqHdr->tableId, reqHdr->indexId, stringKey,
                    reqHdr->keyLength)) {
                respHdr->common.status = STATUS_UNKNOWN_INDEXLET;
                return;
            }
            break;
        }
        case WireFormat::ServerControl::SERVER_ID:
            // No checks are necessary as it is assumed that a ServerId targeted
            // RPC cannot hit the wrong server.
            break;
        default:
            // Return format error if the RpcType is unknown.
            respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
            return;
    }

    uint32_t reqOffset = sizeof32(*reqHdr) + reqHdr->keyLength;
    const void* inputData = rpc->requestPayload->getRange(reqOffset,
                                                          reqHdr->inputLength);

    switch (reqHdr->controlOp) {
        case WireFormat::START_DISPATCH_PROFILER:
        {
            if (rpc->requestPayload->getOffset<uint64_t>(reqOffset) == NULL) {
                respHdr->common.status = STATUS_MESSAGE_TOO_SHORT;
                return;
            }
            const uint64_t* totalElements = (const uint64_t*) inputData;
            context->dispatch->startProfiler(*totalElements);
            break;
        }
        case WireFormat::STOP_DISPATCH_PROFILER:
        {
            context->dispatch->stopProfiler();
            break;
        }
        case WireFormat::DUMP_DISPATCH_PROFILE:
        {
            const char* fileName = (const char*) inputData;
            // Checks to see if the fileName is a properly formatted (zero
            // ended) string.
            if (*(fileName + (reqHdr->inputLength) - 1) != '\0') {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                return;
            }
            try {
                context->dispatch->dumpProfile(fileName);
                break;
            }
            catch(std::ofstream::failure& e) {
                respHdr->common.status = STATUS_REQUEST_FORMAT_ERROR;
                return;
            }
        }
        case WireFormat::GET_PERF_STATS:
        {
            PerfStats stats;
            PerfStats::collectStats(&stats);
            context->getMasterService()->objectManager.getLog()
                   ->getMemoryStats(&stats);
            respHdr->outputLength = sizeof32(stats);
            rpc->replyPayload->appendCopy(&stats, respHdr->outputLength);
            break;
        }
        case WireFormat::GET_TIME_TRACE:
        {
            string s = TimeTrace::getTrace();
            respHdr->outputLength = downCast<uint32_t>(s.length());
            rpc->replyPayload->appendCopy(s.c_str(), respHdr->outputLength);
            break;
        }
        case WireFormat::LOG_MESSAGE:
        {
            const LogLevel* logLevel = (const LogLevel*) inputData;
            if (reqHdr->inputLength < sizeof(LogLevel)
                    || *logLevel >= NUM_LOG_LEVELS) {
                respHdr->common.status = STATUS_INVALID_PARAMETER;
                return;
            }

            uint32_t strlen = reqHdr->inputLength - (uint32_t) sizeof(LogLevel);
            const char* message = ((const char*) inputData) + sizeof(LogLevel);
            LOG(*logLevel, "%.*s", strlen, message);
            break;
        }
        case WireFormat::LOG_TIME_TRACE:
        {
            TimeTrace::printToLog();
            break;
        }
        case WireFormat::GET_CACHE_TRACE:
        {
            string s = context->cacheTrace->getTrace();
            respHdr->outputLength = downCast<uint32_t>(s.length());
            rpc->replyPayload->appendCopy(s.c_str(), respHdr->outputLength);
            break;
        }
        case WireFormat::LOG_BASIC_TRANSPORT_ISSUES:
        {
            BasicTransport::logIssueStats();
            break;
        }
        case WireFormat::LOG_CACHE_TRACE:
        {
            context->cacheTrace->printToLog();
            break;
        }
        case WireFormat::QUIESCE:
        {
            LOG(NOTICE, "Backup is waiting for dirty write buffers to sync");
            if (context->getBackupService() != NULL) {
                context->getBackupService()->storage->quiesce();
            }
            break;
        }
        case WireFormat::RESET_METRICS:
        {
            TimeTrace::reset();
            break;
        }
        case WireFormat::START_PERF_COUNTERS:
        {
            Perf::EnabledCounter::enabled = true;
            break;
        }
        case WireFormat::STOP_PERF_COUNTERS:
        {
            Perf::EnabledCounter::enabled = false;
            break;
        }
        default:
            respHdr->common.status = STATUS_UNIMPLEMENTED_REQUEST;
            return;
    }
}

/**
 * For debugging and testing this function tells the server to kill itself.
 * There will be no response to the RPC for this message, and the process
 * will exit with status code 0.
 *
 * This should only be used for debugging and performance testing.
 */
void
PingService::kill(const WireFormat::Kill::Request* reqHdr,
                  WireFormat::Kill::Response* respHdr,
                  Rpc* rpc)
{
    LOG(ERROR, "Server remotely told to kill itself.");
    if (!ignoreKill)
        exit(0);
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
PingService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::GetMetrics::opcode:
            callHandler<WireFormat::GetMetrics, PingService,
                        &PingService::getMetrics>(rpc);
            break;
        case WireFormat::GetServerId::opcode:
            callHandler<WireFormat::GetServerId, PingService,
                        &PingService::getServerId>(rpc);
            break;
        case WireFormat::Ping::opcode:
            callHandler<WireFormat::Ping, PingService, &PingService::ping>(rpc);
            break;
        case WireFormat::ProxyPing::opcode:
            callHandler<WireFormat::ProxyPing, PingService,
                        &PingService::proxyPing>(rpc);
            break;
        case WireFormat::ServerControl::opcode:
            callHandler<WireFormat::ServerControl, PingService,
                        &PingService::serverControl>(rpc);
            break;
        case WireFormat::Kill::opcode:
            callHandler<WireFormat::Kill, PingService,
                        &PingService::kill>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

} // namespace RAMCloud
