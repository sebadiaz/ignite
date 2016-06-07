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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

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

    /** IGFS context. */
    private final IgfsContext igfsCtx;

    /** Path to file. */
    private final IgfsPath path;

    /** Buffer size. */
    private final int bufSize;

    /** IGFS mode. */
    private final IgfsMode mode;

    /** File worker batch. */
    private final IgfsFileWorkerBatch batch;

    /** Write completion future. */
    private final IgniteInternalFuture<Boolean> writeCompletionFut;

    /** Mutex for synchronization. */
    private final Object mux = new Object();

    /** Flag for this stream open/closed state. */
    private boolean closed;

    /** Local buffer to store stream data as consistent block. */
    private ByteBuffer buf;

    /** Bytes written. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long bytes;

    /** Time consumed by write operations. */
    private long time;

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

    /** Affinity written by this output stream. */
    private IgfsFileAffinityRange streamRange;

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
            writeCompletionFut = igfsCtx.data().writeStart(fileInfo.id());
        }

        igfsCtx.igfs().localMetrics().incrementFilesOpenedForWrite();
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            b &= 0xFF;

            long startTime = System.nanoTime();

            if (buf == null)
                buf = allocateNewBuffer();

            buf.put((byte)b);

            sendBufferIfFull();

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

            // Check if there is anything to write.
            if (len == 0)
                return;

            long startTime = System.nanoTime();

            if (buf == null) {
                if (len >= bufSize) {
                    // Send data right away.
                    ByteBuffer tmpBuf = ByteBuffer.wrap(b, off, len);

                    send(tmpBuf, tmpBuf.remaining());
                }
                else {
                    buf = allocateNewBuffer();

                    buf.put(b, off, len);
                }
            }
            else {
                // Re-allocate buffer if needed.
                if (buf.remaining() < len)
                    buf = ByteBuffer.allocate(buf.position() + len).put((ByteBuffer)buf.flip());

                buf.put(b, off, len);

                sendBufferIfFull();
            }

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @Override public void transferFrom(DataInput in, int len) throws IOException {
        synchronized (mux) {
            checkClosed(in, len);

            long startTime = System.nanoTime();

            // Clean-up local buffer before streaming.
            sendBufferIfNotEmpty();

            // Perform transfer.
            send(in, len);

            time += System.nanoTime() - startTime;
        }
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public void flush() throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            sendBufferIfNotEmpty();

            try {
                flushRemainder();

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
            }
            catch (IgniteCheckedException e) {
                throw new IOException("Failed to flush data [path=" + path + ", space=" + space + ']', e);
            }
        }
    }

    /**
     * Flush remainder.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void flushRemainder() throws IgniteCheckedException {
        if (remainder != null) {
            igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, null, 0,
                ByteBuffer.wrap(remainder, 0, remainderDataLen), true, streamRange, batch);

            remainder = null;
            remainderDataLen = 0;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ThrowFromFinallyBlock")
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
                    if (batch != null)
                        batch.finish();

                    // Ensure file existence.
                    IOException err = null;

                    try {
                        igfsCtx.data().writeClose(fileInfo.id());

                        writeCompletionFut.get();
                    }
                    catch (IgniteCheckedException e) {
                        err = new IOException("Failed to close stream [path=" + path +
                            ", fileInfo=" + fileInfo + ']', e);
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
                        // Safety to ensure that all data blocks are deleted.
                        try {
                            if (mode == DUAL_SYNC)
                                batch.await();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IOException("Failed to close secondary file system stream [path=" + path +
                                ", fileInfo=" + fileInfo + ']', e);
                        }
                        finally {
                            igfsCtx.data().delete(fileInfo);
                        }

                        throw new IOException("File was concurrently deleted: " + path);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IOException("File to read file metadata: " + path, e);
                    }

                    if (err != null)
                        throw err;

                    igfsCtx.igfs().localMetrics().decrementFilesOpenedForWrite();

                    GridEventStorageManager evts = igfsCtx.kernalContext().event();

                    if (evts.isRecordable(EVT_IGFS_FILE_CLOSED_WRITE))
                        evts.record(new IgfsEvent(path, igfsCtx.kernalContext().discovery().localNode(),
                            EVT_IGFS_FILE_CLOSED_WRITE, bytes));
                }
            }
            finally {
                // Mark this stream closed AFTER flush.
                closed = true;
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
     * Send local buffer if it full.
     *
     * @throws IOException If failed.
     */
    private void sendBufferIfFull() throws IOException {
        if (buf.position() >= bufSize)
            sendBuffer();
    }

    /**
     * Send local buffer if at least something is stored there.
     *
     * @throws IOException
     */
    private void sendBufferIfNotEmpty() throws IOException {
        if (buf != null && buf.position() > 0)
            sendBuffer();
    }

    /**
     * Send all local-buffered data to server.
     *
     * @throws IOException In case of IO exception.
     */
    private void sendBuffer() throws IOException {
        buf.flip();

        send(buf, buf.remaining());

        buf = null;
    }

    /**
     * Store data block.
     *
     * @param data Block.
     * @param writeLen Write length.
     * @throws IOException If failed.
     */
    private void send(Object data, int writeLen) throws IOException {
        assert Thread.holdsLock(mux);
        assert data instanceof ByteBuffer || data instanceof DataInput;

        try {
            // Check if pending writes are ok.
            if (writeCompletionFut.isDone()) {
                if (data instanceof DataInput)
                    ((DataInput) data).skipBytes(writeLen);

                writeCompletionFut.get();
            }

            // Increment metrics.
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
                    ((ByteBuffer) data).get(remainder, remainderDataLen, writeLen);
                else
                    ((DataInput) data).readFully(remainder, remainderDataLen, writeLen);

                remainderDataLen += writeLen;
            }
            else {
                if (data instanceof ByteBuffer) {
                    remainder = igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, remainder,
                        remainderDataLen, (ByteBuffer) data, false, streamRange, batch);
                }
                else {
                    remainder = igfsCtx.data().storeDataBlocks(fileInfo, fileInfo.length() + space, remainder,
                        remainderDataLen, (DataInput) data, writeLen, false, streamRange, batch);
                }

                remainderDataLen = remainder == null ? 0 : remainder.length;
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to store data into file: " + path, e);
        }
    }

    /**
     * Allocate new buffer.
     *
     * @return New buffer.
     */
    private ByteBuffer allocateNewBuffer() {
        return ByteBuffer.allocate(bufSize);
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