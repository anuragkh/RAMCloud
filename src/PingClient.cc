/* Copyright (c) 2011-2012 Stanford University
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

#include "Common.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "PingClient.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Issue a trivial RPC to test that a server exists and is responsive.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      If this is a valid ServerId, then the callee will check to see
 *      if this id exists in its server list as an active cluster member.
 *      If not, CallerNotInClusterException will be thrown. If this is an
 *      invalid ServerId (default), then no check is made.
 *
 * \return
 *      If \a serverId had a server list, then its version number is returned;
 *      otherwise zero is returned.
 *
 * \throw ServerNotUpException
 *      The intended server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 * \throw CallerNotInClusterException
 *      CallerId was specified, but the target server doesn't think
 *      it is part of the cluster anymore.
 */ 
void
PingClient::ping(Context* context, ServerId targetId, ServerId callerId)
{
    PingRpc rpc(context, targetId, callerId);
    rpc.wait();
}

/**
 * Constructor for PingRpc: initiates an RPC in the same way as
 * #PingClient::ping, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param targetId
 *      Identifies the server to which the RPC should be sent.
 * \param callerId
 *      If this is a valid ServerId, then the callee will check to see
 *      if this id exists in its server list as an active cluster member.
 *      If not, CallerNotInClusterException will be thrown. If this is an
 *      invalid ServerId (default), then no check is made.
 */
PingRpc::PingRpc(Context* context, ServerId targetId, ServerId callerId)
    : ServerIdRpcWrapper(context, targetId,
            sizeof(WireFormat::Ping::Response))
{
    WireFormat::Ping::Request* reqHdr(
            allocHeader<WireFormat::Ping>());
    reqHdr->callerId = callerId.getId();
    send();
}

/**
 * Wait for a ping RPC to complete, but only wait for a given amount of
 * time, and return if no response is received by then.
 *
 * \param timeoutNanoseconds
 *      If no response is received within this many nanoseconds, then
 *      give up.
 *
 * \return
 *      True is returned if a response was received within
 *      \c timeoutNanoseconds; otherwise, false is returned.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 * \throw CallerNotInClusterException
 *      CallerId was specified, but the target server doesn't think
 *      it is part of the cluster anymore.
 */
bool
PingRpc::wait(uint64_t timeoutNanoseconds)
{
    uint64_t abortTime = Cycles::rdtsc() +
            Cycles::fromNanoseconds(timeoutNanoseconds);
    if (!waitInternal(context->dispatch, abortTime)) {
        TEST_LOG("timeout");
        return false;
    }
    if (serverCrashed) {
        TEST_LOG("server doesn't exist");
        return false;
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
    return true;
}

/**
 * Ask one service to ping another service (useful for checking possible
 * connectivity issues).
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 *
 * \result
 *      The amount of time it took \a targetId to respond to the ping
 *      request from \a proxyId.  If no response was received within
 *      \a timeoutNanoseconds, then all ones is returned.
 *
 * \throw ServerNotUpException
 *      Generated if \a proxyId is not part of the cluster; if it ever
 *      existed, it has since crashed.
 */ 
uint64_t
PingClient::proxyPing(Context* context, ServerId proxyId, ServerId targetId,
        uint64_t timeoutNanoseconds)
{
    ProxyPingRpc rpc(context, proxyId, targetId, timeoutNanoseconds);
    return rpc.wait();
}

/**
 * Constructor for ProxyPingRpc: initiates an RPC in the same way as
 * #PingClient::proxyPing, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server.
 * \param proxyId
 *      Identifies the server to which the RPC should be sent; this server
 *      will ping \a targetId.
 * \param targetId
 *      Identifies the server that \a proxyId will ping.
 * \param timeoutNanoseconds
 *      The maximum amount of time (in nanoseconds) that \a proxyId
 *      will wait for \a targetId to respond.
 */
ProxyPingRpc::ProxyPingRpc(Context* context, ServerId proxyId,
        ServerId targetId, uint64_t timeoutNanoseconds)
    : ServerIdRpcWrapper(context, proxyId,
            sizeof(WireFormat::ProxyPing::Response))
{
    WireFormat::ProxyPing::Request* reqHdr(
            allocHeader<WireFormat::ProxyPing>());
    reqHdr->serverId = targetId.getId();
    reqHdr->timeoutNanoseconds = timeoutNanoseconds;
    send();
}

/**
 * Wait for a proxyPing RPC to complete.
 *
 * \return
 *      The amount of time it took the target server to respond to the ping
 *      request.  If the proxy didn't receive a response within the timeout
 *      period, then all ones is returned.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
uint64_t
ProxyPingRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::ProxyPing::Response* respHdr(
            getResponseHeader<WireFormat::ProxyPing>());
    return respHdr->replyNanoseconds;
}

}  // namespace RAMCloud
