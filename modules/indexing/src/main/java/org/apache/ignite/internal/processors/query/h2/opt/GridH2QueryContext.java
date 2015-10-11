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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.LOCAL;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private static final ThreadLocal<GridH2QueryContext> qctx = new ThreadLocal<>();

    /** */
    private static final ConcurrentMap<Key, GridH2QueryContext> qctxs = new ConcurrentHashMap8<>();

    /** */
    private final Key key;

    /** */
    private final ConcurrentMap<Long, Object> idxSnapshots = new ConcurrentHashMap8<>();

    /** */
    private IndexingQueryFilter filter;

    /** */
    private Map<UUID,int[]> partsMap;

    /** */
    private UUID[] partsNodes;

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     */
    public GridH2QueryContext(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        key = new Key(locNodeId, nodeId, qryId, type);
    }

    /**
     * @param partsMap Partitions map.
     * @return {@code this}.
     */
    public GridH2QueryContext partitionsMap(Map<UUID,int[]> partsMap) {
        this.partsMap = partsMap;

        return this;
    }

    /**
     * @return Partitions map.
     */
    public Map<UUID,int[]> partitionsMap() {
        return partsMap;
    }

    /**
     * @param p Partition.
     * @return Owning node ID.
     */
    public UUID nodeForPartition(int p) {
        UUID[] nodeIds = partsNodes;

        if (nodeIds == null) {
            assert partsMap != null;

            int allParts = 0;

            for (int[] nodeParts : partsMap.values())
                allParts += nodeParts.length;

            nodeIds = new UUID[allParts];

            for (Map.Entry<UUID,int[]> e : partsMap.entrySet()) {
                UUID nodeId = e.getKey();
                int[] nodeParts = e.getValue();

                assert nodeId != null;
                assert !F.isEmpty(nodeParts);

                for (int part : nodeParts) {
                    assert nodeIds[part] == null;

                    nodeIds[part] = nodeId;
                }
            }

            partsNodes = nodeIds;
        }

        return nodeIds[p];
    }

    /**
     * @param idxId Index ID.
     * @param snapshot Index snapshot.
     */
    public void putSnapshot(long idxId, Object snapshot) {
        assert snapshot != null;

        if (idxSnapshots.putIfAbsent(idxId, snapshot) != null)
            throw new IllegalStateException("Index already snapshoted.");
    }

    /**
     * @param idxId Index ID.
     * @return Index snapshot or {@code null} if none.
     */
    @SuppressWarnings("unchecked")
    public <T> T getSnapshot(long idxId) {
        return (T)idxSnapshots.get(idxId);
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
     public static void set(GridH2QueryContext x) {
         assert qctx.get() == null;

         if (x.key.type != LOCAL && qctxs.putIfAbsent(x.key, x) != null)
             throw new IllegalStateException("Query context is already set.");

         qctx.set(x);
    }

    /**
     * Drops current thread local context.
     *
     * @param onlyThreadLoc Drop only thread local context but keep global.
     */
    public static void clear(boolean onlyThreadLoc) {
        GridH2QueryContext x = qctx.get();

        qctx.remove();

        if (!onlyThreadLoc && x.key.type != LOCAL)
            doClear(x.key);
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     */
    public static void clear(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        doClear(new Key(locNodeId, nodeId, qryId, type));
    }

    /**
     * @param key Context key.
     */
    private static void doClear(Key key) {
        GridH2QueryContext qctx0 = qctxs.remove(key);

        if (qctx0 != null) {
            // TODO close all snapshots
        }
    }

    /**
     * Access current thread local query context (if it was set).
     *
     * @return Current thread local query context or {@code null} if the query runs outside of Ignite context.
     */
    @Nullable public static GridH2QueryContext get() {
        return qctx.get();
    }

    /**
     * Access query context from another thread.
     *
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     * @return Query context.
     */
    @Nullable public static GridH2QueryContext get(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        GridH2QueryType type
    ) {
        return qctxs.get(new Key(locNodeId, nodeId, qryId, type));
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     * @return {@code this}.
     */
    public GridH2QueryContext filter(IndexingQueryFilter filter) {
        this.filter = filter;

        return this;
    }

    /**
     * Unique key for the query context.
     */
    private static class Key {
        /** */
        final UUID locNodeId;

        /** */
        final UUID nodeId;

        /** */
        final long qryId;

        /** */
        final GridH2QueryType type;

        /**
         * @param locNodeId Local node ID.
         * @param nodeId The node who initiated the query.
         * @param qryId The query ID.
         * @param type Query type.
         */
        private Key(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
            assert locNodeId != null;
            assert nodeId != null;
            assert type != null;

            this.locNodeId = locNodeId;
            this.nodeId = nodeId;
            this.qryId = qryId;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return qryId == key.qryId && nodeId.equals(key.nodeId) && type == key.type &&
               locNodeId.equals(key.locNodeId) ;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = locNodeId.hashCode();

            result = 31 * result + nodeId.hashCode();
            result = 31 * result + (int)(qryId ^ (qryId >>> 32));
            result = 31 * result + type.hashCode();

            return result;
        }
    }
}