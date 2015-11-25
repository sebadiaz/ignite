/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.ACK_CLOSURE;

/**
 * Grid client for NIO server.
 */
public class GridTcpNioCommunicationClient extends GridAbstractCommunicationClient {
    /** Session. */
    private final GridNioSession ses;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ses Session.
     * @param log Logger.
     */
    public GridTcpNioCommunicationClient(GridNioSession ses, IgniteLogger log) {
        super(null);

        assert ses != null;
        assert log != null;

        this.ses = ses;
        this.log = log;
    }

    /**
     * @return Gets underlying session.
     */
    public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res)
            ses.close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(data);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public boolean sendMessage(@Nullable UUID nodeId, final Message msg, IgniteInClosure<IgniteException> closure)
        throws IgniteCheckedException {
        // Node ID is never provided in asynchronous send mode.
        assert nodeId == null;

        if (closure != null)
            ses.addMeta(ACK_CLOSURE.ordinal(), closure);

        GridNioFuture<?> fut = ses.send(msg);

        if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
            fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(final IgniteInternalFuture<?> future) {
                    U.log(log, "C1>> " + ((GridIoMessage)msg).message() + " " +future);
                }
            });
        }

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (closure != null)
                    ses.removeMeta(ACK_CLOSURE.ordinal());

                if (log.isDebugEnabled())
                    log.debug("Failed to send message [client=" + this + ", err=" + e + ']');

                if (e.getCause() instanceof IOException)
                    return true;
                else
                    throw new IgniteCheckedException("Failed to send message [client=" + this + ']', e);
            }
        }

        if (closure != null)
            ses.removeMeta(ACK_CLOSURE.ordinal());

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean async() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        long now = U.currentTimeMillis();

        // Session can be used for receiving and sending.
        return Math.min(Math.min(now - ses.lastReceiveTime(), now - ses.lastSendScheduleTime()),
            now - ses.lastSendTime());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpNioCommunicationClient.class, this, super.toString());
    }
}