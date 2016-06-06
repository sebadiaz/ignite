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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_CLOSED_WRITE;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
class IgfsOutputStreamImpl extends IgfsOutputStream {
    /** Maximum number of blocks in buffer. */
    private static final int MAX_BLOCKS_CNT = 16;

    /** Path to file. */
    private final IgfsPath path;

    /** Buffer size. */
    private final int bufSize;

    /** Flag for this stream open/closed state. */
    private boolean closed;

    /** Local buffer to store stream data as consistent block. */
    private ByteBuffer buf;

    /** Bytes written. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long bytes;

    /** Time consumed by write operations. */
    private long time;

    /** IGFS context. */
    private IgfsContext igfsCtx;

    /** File descriptor. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IgfsEntryInfo fileInfo;

    /** Space in file to write data. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long space;

    /** Intermediate remainder to keep data. */
    private byte[] remainder;

    /** Data length in remainder. */
    private int remainderDataLen;

    /** Write completion future. */
    private final IgniteInternalFuture<Boolean> writeCompletionFut;

    /** IGFS mode. */
    private final IgfsMode mode;

    /** File worker batch. */
    private final IgfsFileWorkerBatch batch;

    /** Ensures that onClose)_ routine is called no more than once. */
    private final AtomicBoolean onCloseGuard = new AtomicBoolean();

    /** Affinity written by this output stream. */
    private IgfsFileAffinityRange streamRange;

    /** Close guard. */
    private final AtomicBoolean closeGuard = new AtomicBoolean(false);

    /** Mutex for synchronization. */
    private final Object mux = new Object();

    /**
     * Constructs file output stream.
     *
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param fileInfo File info to write binary data to.
     * @param bufSize The size of the buffer to be used.
     * @param mode Grid IGFS mode.
     * @param batch Optional secondary file system batch.
     */
    IgfsOutputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsEntryInfo fileInfo, int bufSize, IgfsMode mode,
        @Nullable IgfsFileWorkerBatch batch) {
        assert fileInfo != null && fileInfo.isFile() : "Unexpected file info: " + fileInfo;
        assert mode != null && mode != PROXY && (mode == PRIMARY && batch == null || batch != null);

        // File hasn't been locked.
        if (fileInfo.lockId() == null)
            throw new IgfsException("Failed to acquire file lock (concurrently modified?): " + path);

        synchronized (mux) {
            this.path = path;
            this.bufSize = optimizeBufferSize(bufSize, fileInfo);
            this.igfsCtx = igfsCtx;
            this.fileInfo = fileInfo;
            this.mode = mode;
            this.batch = batch;

            streamRange = initialStreamRange(fileInfo);
            writeCompletionFut = igfsCtx.data().writeStart(fileInfo);
        }

        igfsCtx.igfs().localMetrics().incrementFilesOpenedForWrite();
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            long startTime = System.nanoTime();

            b &= 0xFF;

            if (buf == null)
                buf = ByteBuffer.allocate(bufSize);

            buf.put((byte)b);

            if (buf.position() >= bufSize)
                sendData(true); // Send data to server.

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void write(byte[] b, int off, int len) throws IOException {
        A.notNull(b, "b");

        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException("Invalid bounds [data.length=" + b.length + ", offset=" + off +
                ", length=" + len + ']');
        }

        synchronized (mux) {
            checkClosed(null, 0);

            if (len == 0)
                return; // Done.

            long startTime = System.nanoTime();

            if (buf == null) {
                // Do not allocate and copy byte buffer if will send data immediately.
                if (len >= bufSize) {
                    buf = ByteBuffer.wrap(b, off, len);

                    sendData(false);

                    return;
                }

                buf = ByteBuffer.allocate(Math.max(bufSize, len));
            }

            if (buf.remaining() < len)
                // Expand buffer capacity, if remaining size is less then data size.
                buf = ByteBuffer.allocate(buf.position() + len).put((ByteBuffer)buf.flip());

            assert len <= buf.remaining() : "Expects write data size less or equal then remaining buffer capacity " +
                "[len=" + len + ", buf.remaining=" + buf.remaining() + ']';

            buf.put(b, off, len);

            if (buf.position() >= bufSize)
                sendData(true); // Send data to server.

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @Override public void transferFrom(DataInput in, int len) throws IOException {
        synchronized (mux) {
            checkClosed(in, len);

            long startTime = System.nanoTime();

            // Send all IPC data from the local buffer before streaming.
            if (buf != null && buf.position() > 0)
                sendData(true);

            try {
                storeData(in, len);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e.getMessage(), e);
            }

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IOException {
        synchronized (mux) {
            // Do nothing if stream is already closed.
            if (closed)
                return;

            try {
                // Send all IPC data from the local buffer.
                try {
                    flush();
                }
                finally {
                    if (closeGuard.compareAndSet(false, true)) {
                        onClose(false);

                        igfsCtx.igfs().localMetrics().decrementFilesOpenedForWrite();

                        GridEventStorageManager evts = igfsCtx.kernalContext().event();

                        if (evts.isRecordable(EVT_IGFS_FILE_CLOSED_WRITE))
                            evts.record(new IgfsEvent(path, igfsCtx.kernalContext().discovery().localNode(),
                                EVT_IGFS_FILE_CLOSED_WRITE, bytes));
                    }
                }
            }
            finally {
                // Mark this stream closed AFTER flush.
                closed = true;
            }
        }
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public void flush() throws IOException {
        synchronized (mux) {

            boolean exists;

            try {
                exists = igfsCtx.meta().exists(fileInfo.id());
            } catch (IgniteCheckedException e) {
                throw new IOException("File to read file metadata: " + path, e);
            }

            if (!exists) {
                onClose(true);

                throw new IOException("File was concurrently deleted: " + path);
            }

            checkClosed(null, 0);

            // Send all IPC data from the local buffer.
            if (buf != null && buf.position() > 0)
                sendData(true);

            try {
                if (remainder != null) {
                    igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, null, 0,
                        ByteBuffer.wrap(remainder, 0, remainderDataLen), true, streamRange, batch);

                    remainder = null;
                    remainderDataLen = 0;
                }

                if (space > 0) {
                    igfsCtx.data().awaitAllAcksReceived(fileInfo.id());

                    IgfsEntryInfo fileInfo0 = igfsCtx.meta().reserveSpace(path, fileInfo.id(), space, streamRange);

                    if (fileInfo0 == null)
                        throw new IOException("File was concurrently deleted: " + path);
                    else
                        fileInfo = fileInfo0;

                    streamRange = initialStreamRange(fileInfo);

                    space = 0;
                }
            } catch (IgniteCheckedException e) {
                throw new IOException("Failed to flush data [path=" + path + ", space=" + space + ']', e);
            }
        }
    }

    /**
     * Store data block.
     *
     * @param data Block.
     * @param writeLen Write length.
     * @throws IgniteCheckedException If failed.
     * @throws IOException If failed.
     */
    private void storeData(Object data, int writeLen) throws IgniteCheckedException, IOException {
        assert Thread.holdsLock(mux);
        assert data instanceof ByteBuffer || data instanceof DataInput;

        if (writeCompletionFut.isDone()) {
            assert ((GridFutureAdapter)writeCompletionFut).isFailed();

            writeCompletionFut.get();
        }

        bytes += writeLen;
        space += writeLen;

        int blockSize = fileInfo.blockSize();

        // If data length is not enough to fill full block, fill the remainder and return.
        if (remainderDataLen + writeLen < blockSize) {
            if (remainder == null)
                remainder = new byte[blockSize];
            else if (remainder.length != blockSize) {
                assert remainderDataLen == remainder.length;

                byte[] allocated = new byte[blockSize];

                U.arrayCopy(remainder, 0, allocated, 0, remainder.length);

                remainder = allocated;
            }

            if (data instanceof ByteBuffer)
                ((ByteBuffer)data).get(remainder, remainderDataLen, writeLen);
            else
                ((DataInput)data).readFully(remainder, remainderDataLen, writeLen);

            remainderDataLen += writeLen;
        }
        else {
            if (data instanceof ByteBuffer) {
                remainder = igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, remainder,
                    remainderDataLen, (ByteBuffer)data, false, streamRange, batch);
            }
            else {
                remainder = igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, remainder,
                    remainderDataLen, (DataInput)data, writeLen, false, streamRange, batch);
            }

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /**
     * Close callback. It will be called only once in synchronized section.
     *
     * @param deleted Whether we already know that the file was deleted.
     * @throws IOException If failed.
     */
    private void onClose(boolean deleted) throws IOException {
        assert Thread.holdsLock(mux);

        if (onCloseGuard.compareAndSet(false, true)) {
            // Notify backing secondary file system batch to finish.
            if (batch != null)
                batch.finish();

            // Ensure file existence.
            boolean exists;

            try {
                exists = !deleted && igfsCtx.meta().exists(fileInfo.id());
            }
            catch (IgniteCheckedException e) {
                throw new IOException("File to read file metadata: " + path, e);
            }

            if (exists) {
                IOException err = null;

                try {
                    igfsCtx.data().writeClose(fileInfo);

                    writeCompletionFut.get();
                }
                catch (IgniteCheckedException e) {
                    err = new IOException("Failed to close stream [path=" + path + ", fileInfo=" + fileInfo + ']', e);
                }

                igfsCtx.igfs().localMetrics().addWrittenBytesTime(bytes, time);

                // Await secondary file system processing to finish.
                if (mode == DUAL_SYNC) {
                    try {
                        assert batch != null;

                        batch.await();
                    }
                    catch (IgniteCheckedException e) {
                        if (err == null)
                            err = new IOException("Failed to close secondary file system stream [path=" + path +
                                ", fileInfo=" + fileInfo + ']', e);
                    }
                }

                long modificationTime = System.currentTimeMillis();

                try {
                    igfsCtx.meta().unlock(fileInfo, modificationTime);
                }
                catch (IgfsPathNotFoundException ignore) {
                    igfsCtx.data().delete(fileInfo); // Safety to ensure that all data blocks are deleted.

                    throw new IOException("File was concurrently deleted: " + path);
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("File to read file metadata: " + path, e);
                }

                if (err != null)
                    throw err;
            }
            else {
                try {
                    if (mode == DUAL_SYNC) {
                        assert batch != null;

                        batch.await();
                    }
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to close secondary file system stream [path=" + path +
                        ", fileInfo=" + fileInfo + ']', e);
                }
                finally {
                    igfsCtx.data().delete(fileInfo);
                }
            }
        }
    }

    /**
     * Validate this stream is open.
     *
     * @throws IOException If this stream is closed.
     */
    private void checkClosed(@Nullable DataInput in, int len) throws IOException {
        assert Thread.holdsLock(mux);

        if (closed) {
            // Must read data from stream before throwing exception.
            if (in != null)
                in.skipBytes(len);

            throw new IOException("Stream has been closed: " + this);
        }
    }

    /**
     * Send all local-buffered data to server.
     *
     * @param flip Whether to flip buffer on sending data. We do not want to flip it if sending wrapped
     *      byte array.
     * @throws IOException In case of IO exception.
     */
    private void sendData(boolean flip) throws IOException {
        assert Thread.holdsLock(mux);

        try {
            if (flip)
                buf.flip();

            storeData(buf, buf.remaining());

            buf = null;
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to store data into file: " + path, e);
        }
    }

    /**
     * Gets initial affinity range. This range will have 0 length and will start from first
     * non-occupied file block.
     *
     * @param fileInfo File info to build initial range for.
     * @return Affinity range.
     */
    private IgfsFileAffinityRange initialStreamRange(IgfsEntryInfo fileInfo) {
        if (!igfsCtx.configuration().isFragmentizerEnabled())
            return null;

        if (!Boolean.parseBoolean(fileInfo.properties().get(IgfsUtils.PROP_PREFER_LOCAL_WRITES)))
            return null;

        int blockSize = fileInfo.blockSize();

        // Find first non-occupied block offset.
        long off = ((fileInfo.length() + blockSize - 1) / blockSize) * blockSize;

        // Need to get last affinity key and reuse it if we are on the same node.
        long lastBlockOff = off - fileInfo.blockSize();

        if (lastBlockOff < 0)
            lastBlockOff = 0;

        IgfsFileMap map = fileInfo.fileMap();

        IgniteUuid prevAffKey = map == null ? null : map.affinityKey(lastBlockOff, false);

        IgniteUuid affKey = igfsCtx.data().nextAffinityKey(prevAffKey);

        return affKey == null ? null : new IgfsFileAffinityRange(off, off, affKey);
    }

    /**
     * Optimize buffer size.
     *
     * @param bufSize Requested buffer size.
     * @param fileInfo File info.
     * @return Optimized buffer size.
     */
    private static int optimizeBufferSize(int bufSize, IgfsEntryInfo fileInfo) {
        assert bufSize > 0;

        if (fileInfo == null)
            return bufSize;

        int blockSize = fileInfo.blockSize();

        if (blockSize <= 0)
            return bufSize;

        if (bufSize <= blockSize)
            // Optimize minimum buffer size to be equal file's block size.
            return blockSize;

        int maxBufSize = blockSize * MAX_BLOCKS_CNT;

        if (bufSize > maxBufSize)
            // There is no profit or optimization from larger buffers.
            return maxBufSize;

        if (fileInfo.length() == 0)
            // Make buffer size multiple of block size (optimized for new files).
            return bufSize / blockSize * blockSize;

        return bufSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamImpl.class, this);
    }
}