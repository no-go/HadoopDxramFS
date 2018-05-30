/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.mem;

import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMRuntimeException;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.soh.MemoryRuntimeException;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

/**
 * Interface to access the local heap. Features for migration
 * and other tasks are provided as well.
 * Using this class, you have to take care of locking certain calls.
 * This depends on the type (access or manage). Check the documentation
 * of each call to figure how to handle them. Make use of this by combining
 * multiple calls within a single critical section to avoid locking overhead.
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class MemoryManagerComponent extends AbstractDXRAMComponent<MemoryManagerComponentConfig> {
    static final ThroughputPool SOP_MALLOC = new ThroughputPool(MemoryManagerComponent.class, "Malloc",
            Value.Base.B_10);

    private static final ThroughputPool SOP_MULTI_MALLOC = new ThroughputPool(MemoryManagerComponent.class,
            "MultiMalloc", Value.Base.B_10);
    private static final ThroughputPool SOP_FREE = new ThroughputPool(MemoryManagerComponent.class, "Free",
            Value.Base.B_10);
    private static final ThroughputPool SOP_GET = new ThroughputPool(MemoryManagerComponent.class, "Get",
            Value.Base.B_10);
    private static final ThroughputPool SOP_PUT = new ThroughputPool(MemoryManagerComponent.class, "Put",
            Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE = new ThroughputPool(MemoryManagerComponent.class, "Create",
            Value.Base.B_10);
    private static final ThroughputPool SOP_MULTI_CREATE = new ThroughputPool(MemoryManagerComponent.class,
            "MultiCreate", Value.Base.B_10);
    private static final ThroughputPool SOP_REMOVE = new ThroughputPool(MemoryManagerComponent.class, "Remove",
            Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE_PUT_RECOVERED = new ThroughputPool(MemoryManagerComponent.class,
            "CreateAndPutRecovered", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_MALLOC);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_MULTI_MALLOC);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_FREE);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_GET);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_PUT);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_CREATE);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_MULTI_CREATE);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_REMOVE);
        StatisticsManager.get().registerOperation(MemoryManagerComponent.class, SOP_CREATE_PUT_RECOVERED);
    }

    // component dependencies
    private AbstractBootComponent m_boot;
    private SmallObjectHeap m_rawMemory;
    private CIDTable m_cidTable;
    //private ReentrantReadWriteLock m_lock;
    private AtomicInteger m_lock;
    private long m_numActiveChunks;
    private long m_totalActiveChunkMemory;
    private SmallObjectHeapDataStructureImExporter[] m_imexporter = new SmallObjectHeapDataStructureImExporter[65536];

    /**
     * Constructor
     */
    public MemoryManagerComponent() {
        super(DXRAMComponentOrder.Init.MEMORY, DXRAMComponentOrder.Shutdown.MEMORY, MemoryManagerComponentConfig.class);
    }

    /**
     * Get some status information about the memory manager (free, total amount of memory).
     *
     * @return Status information.
     */
    public Status getStatus() {
        Status status = new Status();

        status.m_freeMemory = new StorageUnit(m_rawMemory.getStatus().getFree(), StorageUnit.BYTE);
        status.m_maxChunkSize = new StorageUnit(m_rawMemory.getStatus().getMaxBlockSize(), StorageUnit.BYTE);
        status.m_totalMemory = new StorageUnit(m_rawMemory.getStatus().getSize(), StorageUnit.BYTE);
        status.m_totalPayloadMemory = new StorageUnit(m_rawMemory.getStatus().getAllocatedPayload(), StorageUnit.BYTE);
        status.m_numberOfActiveMemoryBlocks = m_rawMemory.getStatus().getAllocatedBlocks();
        status.m_totalChunkPayloadMemory = new StorageUnit(m_totalActiveChunkMemory, StorageUnit.BYTE);
        status.m_numberOfActiveChunks = m_numActiveChunks;
        status.m_cidTableCount = m_cidTable.getTableCount();
        status.m_totalMemoryCIDTables = new StorageUnit(m_cidTable.getTotalMemoryTables(), StorageUnit.BYTE);
        status.m_cachedFreeLIDs = m_cidTable.getNumCachedFreeLIDs();
        status.m_availableFreeLIDs = m_cidTable.getNumAvailableFreeLIDs();
        status.m_newLIDCounter = m_cidTable.getNextLocalIDCounter();

        return status;
    }

    /**
     * Returns the highest LocalID currently in use
     *
     * @return the LocalID
     */
    public long getHighestUsedLocalID() {
        return m_cidTable.getNextLocalIDCounter() - 1;
    }

    /**
     * Returns the ChunkID ranges of all migrated Chunks
     *
     * @return the ChunkID ranges of all migrated Chunks
     */
    public ChunkIDRanges getCIDRangesOfAllMigratedChunks() {
        return m_cidTable.getCIDRangesOfAllMigratedChunks();
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @return the ChunkID ranges
     */
    public ChunkIDRanges getCIDRangesOfAllLocalChunks() {
        return m_cidTable.getCIDRangesOfAllLocalChunks();
    }

    /**
     * Lock the memory for a management task (create, put, remove).
     */
    public void lockManage() {
        //m_lock.writeLock().lock();

        do {
            int v = m_lock.get();
            m_lock.compareAndSet(v, v | 0x40000000);
        } while (!m_lock.compareAndSet(0x40000000, 0x80000000));
    }

    /**
     * Lock the memory for an access task (get).
     */
    public void lockAccess() {
        //m_lock.readLock().lock();

        while (true) {
            int v = m_lock.get() & 0x3FFFFFFF;
            if (m_lock.compareAndSet(v, v + 1)) {
                break;
            }
        }
    }

    /**
     * Unlock the memory after a management task (create, put, remove).
     */
    public void unlockManage() {
        //m_lock.writeLock().unlock();

        m_lock.set(0);
    }

    /**
     * Unlock the memory after an access task (get).
     */
    public void unlockAccess() {
        //m_lock.readLock().unlock();

        m_lock.decrementAndGet();
    }

    // -----------------------------------------------------------------------------

    /**
     * The chunk ID 0 is reserved for a fixed index structure.
     * If the index structure is already created this will delete the old
     * one and allocate a new block of memory with the same id (0).
     *
     * @param p_size
     *         Size for the index chunk.
     * @return The chunk id 0
     */
    public long createIndex(final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        try {
            if (m_cidTable.get(0) != 0) {
                // delete old entry
                address = m_cidTable.delete(0, false);
                m_rawMemory.free(address);
                m_totalActiveChunkMemory -= m_rawMemory.getSizeBlock(address);
                m_numActiveChunks--;
            }

            address = m_rawMemory.malloc(p_size);
            if (address >= 0) {
                chunkID = (long) m_boot.getNodeID() << 48;
                // register new chunk in cid table
                if (!m_cidTable.set(chunkID, address)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address);
                    throw new OutOfKeyValueStoreMemoryException(getStatus());
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_size;
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Create a chunk with a specific chunk id (used for migration/recovery).
     *
     * @param p_chunkId
     *         Chunk id to assign to the chunk.
     * @param p_size
     *         Size of the chunk.
     * @return The chunk id if successful, -1 if another chunk with the same id already exists.
     */
    public long create(final long p_chunkId, final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            SOP_CREATE.start(1);
            // #endif /* STATISTICS */

            chunkID = p_chunkId;

            // verify this id is not used
            if (m_cidTable.get(p_chunkId) == 0) {
                address = m_rawMemory.malloc(p_size);
                if (address >= 0) {
                    // register new chunk
                    // register new chunk in cid table
                    if (!m_cidTable.set(chunkID, address)) {
                        // on demand allocation of new table failed
                        // free previously created chunk for data to avoid memory leak
                        m_rawMemory.free(address);
                        throw new OutOfKeyValueStoreMemoryException(getStatus());
                    } else {
                        m_numActiveChunks++;
                        m_totalActiveChunkMemory += p_size;
                        chunkID = p_chunkId;
                    }
                } else {
                    throw new OutOfKeyValueStoreMemoryException(getStatus());
                }
            }

            // #ifdef STATISTICS
            SOP_CREATE.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
    public long[] createMultiSizes(final int... p_sizes) {
        return createMultiSizes(false, p_sizes);
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
    public long[] createMultiSizes(final boolean p_consecutive, final int... p_sizes) {
        long[] addresses;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            SOP_MULTI_CREATE.start(p_sizes.length);
            // #endif /* STATISTICS */

            // get new LIDs
            lids = m_cidTable.getFreeLIDs(p_sizes.length, p_consecutive);

            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.start(p_sizes.length);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizes(p_sizes);
            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.stop();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < lids.length; i++) {
                    lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];

                    // register new chunk in cid table
                    if (!m_cidTable.set(lids[i], addresses[i])) {

                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            m_rawMemory.free(addresses[j]);
                        }

                        throw new OutOfKeyValueStoreMemoryException(getStatus());
                    } else {
                        m_numActiveChunks++;
                        m_totalActiveChunkMemory += p_sizes[i];
                    }
                }

            } else {
                // put lids back
                for (int i = 0; i < lids.length; i++) {
                    m_cidTable.putChunkIDForReuse(lids[i]);
                }

                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }

            // #ifdef STATISTICS
            SOP_MULTI_CREATE.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        return lids;
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    public void createMulti(final DataStructure... p_dataStructures) {
        createMulti(false, p_dataStructures);
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    public void createMulti(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        long[] ids = createMultiSizes(p_consecutive, sizes);

        for (int i = 0; i < ids.length; i++) {
            p_dataStructures[i].setID(ids[i]);
        }
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @return Chunk id list of the created chunks
     */
    public long[] createMulti(final int p_size, final int p_count) {
        return createMulti(p_size, p_count, false);
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @return Chunk id list of the created chunks
     */
    public long[] createMulti(final int p_size, final int p_count, final boolean p_consecutive) {
        long[] addresses;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            SOP_MULTI_CREATE.start(p_count);
            // #endif /* STATISTICS */

            // get new LIDs
            lids = m_cidTable.getFreeLIDs(p_count, p_consecutive);

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.start(p_count);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMalloc(p_size, p_count);
            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.stop();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < lids.length; i++) {
                    lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];

                    // register new chunk in cid table
                    if (!m_cidTable.set(lids[i], addresses[i])) {

                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            m_rawMemory.free(addresses[j]);
                        }

                        throw new OutOfKeyValueStoreMemoryException(getStatus());
                    } else {
                        m_numActiveChunks++;
                        m_totalActiveChunkMemory += p_size;
                    }
                }

            } else {
                // put lids back
                for (int i = 0; i < lids.length; i++) {
                    m_cidTable.putChunkIDForReuse(lids[i]);
                }

                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }

            // #ifdef STATISTICS
            SOP_MULTI_CREATE.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        return lids;
    }

    /**
     * Create a new chunk.
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_size
     *         Size in bytes of the payload the chunk contains.
     * @return Chunk ID for the allocated chunk
     */
    public long create(final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID;
        long lid;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            SOP_CREATE.start(1);
            // #endif /* STATISTICS */

            // get new LID from CIDTable
            lid = m_cidTable.getFreeLID();

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            SOP_MALLOC.start(1);
            // #endif /* STATISTICS */
            address = m_rawMemory.malloc(p_size);
            // #ifdef STATISTICS
            SOP_MALLOC.stop();
            // #endif /* STATISTICS */
            if (address >= 0) {
                chunkID = ((long) m_boot.getNodeID() << 48) + lid;
                // register new chunk in cid table
                if (!m_cidTable.set(chunkID, address)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address);

                    throw new OutOfKeyValueStoreMemoryException(getStatus());
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_size;
                }
            } else {
                // put lid back
                m_cidTable.putChunkIDForReuse(lid);

                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }

            // #ifdef STATISTICS
            SOP_CREATE.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Get the payload of a chunk/data structure.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_dataStructure
     *         Data structure to read specified by its ID.
     * @return True if getting the chunk payload was successful, false if no chunk with the ID specified exists.
     */
    public boolean get(final DataStructure p_dataStructure) {
        long address;
        boolean ret = true;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        try {
            if (p_dataStructure.getID() == ChunkID.INVALID_ID) {
                p_dataStructure.setState(ChunkState.INVALID_ID);
                ret = false;
            } else {
                // #ifdef STATISTICS
                SOP_GET.start(1);
                // #endif /* STATISTICS */

                address = m_cidTable.get(p_dataStructure.getID());
                if (address > 0) {
                    assert m_rawMemory.getSizeBlock(address) == p_dataStructure.sizeofObject();

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = getImExporter(address);
                    importer.importObject(p_dataStructure);

                    p_dataStructure.setState(ChunkState.OK);
                } else {
                    ret = false;
                    p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                }

                // #ifdef STATISTICS
                SOP_GET.stop();
                // #endif /* STATISTICS */
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *         Read the chunk data of the specified ID
     * @return A byte array with payload if getting the chunk payload was successful, null if no chunk with the ID
     * exists.
     */
    public byte[] get(final long p_chunkID) {
        byte[] ret;
        long address;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        try {
            if (p_chunkID == ChunkID.INVALID_ID) {
                ret = null;
            } else {
                // #ifdef STATISTICS
                SOP_GET.start(1);
                // #endif /* STATISTICS */

                address = m_cidTable.get(p_chunkID);
                if (address > 0) {
                    int chunkSize = m_rawMemory.getSizeBlock(address);
                    ret = new byte[chunkSize];

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = getImExporter(address);
                    int retSize = importer.readBytes(ret);
                    if (retSize != chunkSize) {
                        throw new DXRAMRuntimeException("Unknown error, importer size " + retSize + " != chunk size " +
                                chunkSize);
                    }
                } else {
                    ret = null;
                }

                // #ifdef STATISTICS
                SOP_GET.stop();
                // #endif /* STATISTICS */
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *         Read the chunk data of the specified ID
     * @return the number of read bytes
     */
    public int get(final long p_chunkID, final byte[] p_buffer, final int p_offset, final int p_bufferSize) {
        int ret;
        long address;

        try {
            if (p_chunkID == ChunkID.INVALID_ID) {
                return -1;
            }

            // #ifdef STATISTICS
            SOP_GET.start(1);
            // #endif /* STATISTICS */

            address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                int chunkSize = m_rawMemory.getSizeBlock(address);

                if (p_offset + chunkSize > p_bufferSize) {
                    ret = 0;
                } else {
                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = getImExporter(address);
                    ret = importer.readBytes(p_buffer, p_offset, chunkSize);
                    if (ret != chunkSize) {
                        throw new DXRAMRuntimeException("Unknown error, importer size " + ret + " != chunk size " +
                                chunkSize);
                    }
                }
            } else {
                ret = -1;
            }

            // #ifdef STATISTICS
            SOP_GET.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        return ret;
    }

    /**
     * Put data of the a data structure/chunk to the memory
     * This is an access call and has to be locked using lockAccess().
     * Note: lockAccess() does NOT take care of data races of the data to write.
     * The caller has to take care of proper locking to avoid consistency issue with his data.
     *
     * @param p_dataStructure
     *         Data structure to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final DataStructure p_dataStructure) {
        long address;
        boolean ret = true;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        try {
            if (p_dataStructure.getID() == ChunkID.INVALID_ID) {
                p_dataStructure.setState(ChunkState.INVALID_ID);
                ret = false;
            } else {
                // #ifdef STATISTICS
                SOP_PUT.start(1);
                // #endif /* STATISTICS */

                address = m_cidTable.get(p_dataStructure.getID());
                if (address > 0) {
                    assert m_rawMemory.getSizeBlock(address) == p_dataStructure.sizeofObject();

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter exporter = getImExporter(address);
                    exporter.exportObject(p_dataStructure);

                    p_dataStructure.setState(ChunkState.OK);
                } else {
                    ret = false;
                    p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                }

                // #ifdef STATISTICS
                SOP_PUT.stop();
                // #endif /* STATISTICS */
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Put some data into a chunk.
     * This is an access call and has to be locked using lockAccess().
     * Note: lockAccess() does NOT take care of data races of the data to write.
     * The caller has to take care of proper locking to avoid consistency issue with his data.
     *
     * @param p_chunkID
     *         Chunk ID for the data to put
     * @param p_data
     *         Chunk data to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data) {
        return put(p_chunkID, p_data, 0, p_data.length);
    }

    /**
     * Put some data into a chunk.
     * This is an access call and has to be locked using lockAccess().
     * Note: lockAccess() does NOT take care of data races of the data to write.
     * The caller has to take care of proper locking to avoid consistency issue with his data.
     *
     * @param p_chunkID
     *         Chunk ID for the data to put
     * @param p_data
     *         Chunk data to put
     * @param p_offset
     *         Offset for p_data array
     * @param p_length
     *         Number of bytes to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data, final int p_offset, final int p_length) {
        long address;
        boolean ret = true;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER put p_chunkID 0x%X, p_offset %d, p_length %d", p_chunkID, p_offset, p_length);
        // #endif /* LOGGER == TRACE */

        try {
            if (p_chunkID == ChunkID.INVALID_ID) {
                ret = false;
            } else {
                // #ifdef STATISTICS
                SOP_PUT.start(1);
                // #endif /* STATISTICS */

                address = m_cidTable.get(p_chunkID);
                if (address > 0) {
                    assert p_offset + p_length <= m_rawMemory.getSizeBlock(address);

                    m_rawMemory.writeBytes(address, 0, p_data, p_offset, p_length);
                } else {
                    ret = false;
                }

                // #ifdef STATISTICS
                SOP_PUT.stop();
                // #endif /* STATISTICS */
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT put p_chunkID 0x%X, p_offset %d, p_length %d", p_chunkID, p_offset, p_length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Removes a Chunk from the memory
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkID
     *         the ChunkID of the Chunk
     * @param p_wasMigrated
     *         default value for this parameter should be false!
     *         if chunk was deleted during migration this flag should be set to true
     * @return The size of the deleted chunk if removing the data was successful, -1 if the chunk with the specified id
     * does not exist
     */
    public int remove(final long p_chunkID, final boolean p_wasMigrated) {
        int ret = -1;
        long addressDeletedChunk;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        try {
            if (p_chunkID == ChunkID.INVALID_ID) {
                ret = -1;
            } else {
                // #ifdef STATISTICS
                SOP_REMOVE.start(1);
                // #endif /* STATISTICS */

                // Get and delete the address from the CIDTable, mark as zombie first
                addressDeletedChunk = m_cidTable.delete(p_chunkID, true);
                if (addressDeletedChunk > 0) {

                    if (p_wasMigrated) {
                        // deleted and previously migrated chunks don't end up in the LID store
                        m_cidTable.delete(p_chunkID, false);
                    } else {
                        // more space for another zombie for reuse in LID store?
                        if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                            // kill zombie entry
                            m_cidTable.delete(p_chunkID, false);
                        } else {
                            // no space for zombie in LID store, keep him "alive" in table
                        }
                    }
                    ret = m_rawMemory.getSizeBlock(addressDeletedChunk);
                    // #ifdef STATISTICS
                    SOP_FREE.start(1);
                    // #endif /* STATISTICS */
                    m_rawMemory.free(addressDeletedChunk);
                    // #ifdef STATISTICS
                    SOP_FREE.stop();
                    // #endif /* STATISTICS */
                    m_numActiveChunks--;
                    m_totalActiveChunkMemory -= ret;
                }

                // #ifdef STATISTICS
                SOP_REMOVE.stop();
                // #endif /* STATISTICS */
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkIDs
     *         List of recovered chunk ids
     * @param p_dataAddress
     *         The address of the recovered data block
     * @param p_offsets
     *         Offset list for chunks to address the data array
     * @param p_lengths
     *         List of chunk sizes
     * @param p_usedEntries
     *         Specifies the actual number of slots used in the array (may be less than p_lengths)
     */
    public void createAndPutRecovered(final long[] p_chunkIDs, final long p_dataAddress, final int[] p_offsets,
            final int[] p_lengths, final int p_usedEntries) {
        long[] addresses;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            SOP_CREATE_PUT_RECOVERED.start(p_usedEntries);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.start(p_usedEntries);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizesUsedEntries(p_usedEntries, p_lengths);
            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.stop();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < addresses.length; i++) {
                    m_rawMemory.writeBytes(addresses[i], 0, p_dataAddress, p_offsets[i], p_lengths[i]);
                    m_totalActiveChunkMemory += p_lengths[i];
                }

                m_numActiveChunks += addresses.length;

                for (int i = 0; i < addresses.length; i++) {
                    m_cidTable.set(p_chunkIDs[i], addresses[i]);
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }

            // #ifdef STATISTICS
            SOP_CREATE_PUT_RECOVERED.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_dataStructures
     *         All data structure to create and put
     * @return number of written bytes
     */
    public int createAndPutRecovered(final DataStructure... p_dataStructures) {
        int ret = 0;
        long[] addresses;
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        try {
            // #ifdef STATISTICS
            SOP_CREATE_PUT_RECOVERED.start(p_dataStructures.length);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.start(p_dataStructures.length);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizes(sizes);
            // #ifdef STATISTICS
            SOP_MULTI_MALLOC.stop();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < addresses.length; i++) {
                    SmallObjectHeapDataStructureImExporter exporter = getImExporter(addresses[i]);
                    exporter.exportObject(p_dataStructures[i]);
                    ret += sizes[i];
                    m_totalActiveChunkMemory += sizes[i];
                }

                m_numActiveChunks += addresses.length;

                for (int i = 0; i < addresses.length; i++) {
                    m_cidTable.set(p_dataStructures[i].getID(), addresses[i]);
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(getStatus());
            }

            // #ifdef STATISTICS
            SOP_CREATE_PUT_RECOVERED.stop();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, false);
            throw e;
        }

        return ret;
    }

    /**
     * Create a full heap dump
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_fileName
     *         Name of the file to write the dump to
     */
    public void dumpMemory(final String p_fileName) {
        m_rawMemory.dump(p_fileName);
    }

    /**
     * Reset the whole memory, i.e. wipe it (all chunks and IDs gone) and re-init
     */
    public void reset() {
        // #if LOGGER == ERROR
        LOGGER.warn("Resetting FULL memory");
        // #endif /* LOGGER == ERROR */

        shutdownMemory();
        initMemory();
    }

    // -----------------------------------------------------------------------------

    /**
     * Read a single byte from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public byte readByte(final long p_chunkID, final int p_offset) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                return m_rawMemory.readByte(address, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single short from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public short readShort(final long p_chunkID, final int p_offset) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                return m_rawMemory.readShort(address, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single int from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public int readInt(final long p_chunkID, final int p_offset) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                return m_rawMemory.readInt(address, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single long from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public long readLong(final long p_chunkID, final int p_offset) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                return m_rawMemory.readLong(address, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Write a single byte to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeByte(final long p_chunkID, final int p_offset, final byte p_value) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                m_rawMemory.writeByte(address, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single short to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeShort(final long p_chunkID, final int p_offset, final short p_value) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                m_rawMemory.writeShort(address, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single int to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeInt(final long p_chunkID, final int p_offset, final int p_value) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                m_rawMemory.writeInt(address, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single long to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeLong(final long p_chunkID, final int p_offset, final long p_value) {
        try {
            long address = m_cidTable.get(p_chunkID);
            if (address > 0) {
                m_rawMemory.writeLong(address, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    // -----------------------------------------------------------------------------

    /**
     * Returns whether this Chunk is stored locally or not.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *         the ChunkID
     * @return whether this Chunk is stored locally or not
     */
    public boolean exists(final long p_chunkID) {
        long address;

        try {
            // Get the address from the CIDTable
            address = m_cidTable.get(p_chunkID);
        } catch (final MemoryRuntimeException e) {
            handleMemDumpOnError(e, true);
            throw e;
        }

        // If address <= 0, the Chunk does not exists in memory
        return address > 0;
    }

    /**
     * Returns whether this Chunk was migrated here or not
     *
     * @param p_chunkID
     *         the ChunkID
     * @return whether this Chunk was migrated here or not
     */
    public boolean dataWasMigrated(final long p_chunkID) {
        return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
    }

    /**
     * Removes the ChunkID of a deleted Chunk that was migrated
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkID
     *         the ChunkID
     */
    public void prepareChunkIDForReuse(final long p_chunkID) {
        // more space for another zombie for reuse in LID store?
        if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
            // kill zombie entry
            m_cidTable.delete(p_chunkID, false);
        } else {
            // no space for zombie in LID store, keep him "alive" in table
            m_cidTable.delete(p_chunkID, true);
        }
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        initMemory();

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        shutdownMemory();

        return true;
    }

    /**
     * Initialize the memory manager
     */
    private void initMemory() {
        // #if LOGGER == INFO
        LOGGER.info("Allocating native memory (%d mb). This may take a while...",
                getConfig().getKeyValueStoreSize().getMB());
        // #endif /* LOGGER == INFO */
        // Runtime.getRuntime().load("/home/nothaas/dxram/jni/libJNINativeMemory.so");
        m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory(), getConfig().getKeyValueStoreSize().getBytes(),
                (int) getConfig().getKeyValueStoreMaxBlockSize().getBytes());
        m_cidTable = new CIDTable(m_boot.getNodeID());
        m_cidTable.initialize(m_rawMemory);

        m_lock = new AtomicInteger(0);
        //m_lock = new ReentrantReadWriteLock(false);

        m_numActiveChunks = 0;
        m_totalActiveChunkMemory = 0;
    }

    /**
     * Shut down the memory manager
     */
    private void shutdownMemory() {
        m_cidTable.disengage();
        m_rawMemory.destroy();

        m_cidTable = null;
        m_rawMemory = null;
        m_lock = null;
    }

    /**
     * Pooling the im/exporters to lower memory footprint.
     *
     * @param p_address
     *         Start address of the chunk
     * @return Im/Exporter for the chunk
     */
    private SmallObjectHeapDataStructureImExporter getImExporter(final long p_address) {
        long tid = Thread.currentThread().getId();
        if (tid > 65536) {
            throw new RuntimeException("Exceeded max. thread id");
        }

        // pool the im/exporters
        SmallObjectHeapDataStructureImExporter importer = m_imexporter[(int) tid];
        if (importer == null) {
            m_imexporter[(int) tid] = new SmallObjectHeapDataStructureImExporter(m_rawMemory, p_address, 0);
            importer = m_imexporter[(int) tid];
        } else {
            importer.setAllocatedMemoryStartAddress(p_address);
            importer.setOffset(0);
        }

        return importer;
    }

    /**
     * Execute a memory dump (if enabled) on a memory error (corruption)
     * Note: MemoryRuntimeException is only thrown if assertions are enabled (disabled for performance)
     * Otherwise, some memory access errors result in segmentation faults, others aren't detected.
     *
     * @param p_e
     *         Exception thrown on memory error
     */
    private void handleMemDumpOnError(final MemoryRuntimeException p_e, final boolean p_acquireManageLock) {
        // #if LOGGER == ERROR
        LOGGER.fatal("Encountered memory error (most likely corruption)", p_e);
        // #endif /* LOGGER == ERROR */

        if (!getConfig().getMemDumpFolderOnError().isEmpty()) {
            String folder = getConfig().getMemDumpFolderOnError();

            if (!folder.endsWith("/")) {
                folder += "/";
            }

            // create unique file name for each thread to avoid collisions
            String fileName = folder + "memdump-" + Thread.currentThread().getId() + '-' + System.currentTimeMillis() +
                    ".soh";

            // #if LOGGER == ERROR
            LOGGER.fatal("Full memory dump to file: %s...", fileName);
            // #endif /* LOGGER == ERROR */

            // ugly: we entered this with a access lock, acquire the managed lock to ensure full blocking of the memory
            // before dumping
            if (p_acquireManageLock) {
                unlockAccess();
                lockManage();
            }

            // #if LOGGER == ERROR
            LOGGER.fatal("Dumping...");
            // #endif /* LOGGER == ERROR */
            m_rawMemory.dump(fileName);

            if (p_acquireManageLock) {
                unlockManage();
                lockAccess();
            }

            // #if LOGGER == ERROR
            LOGGER.fatal("Memory dump to file finished: %s", fileName);
            // #endif /* LOGGER == ERROR */
        } else {
            // #if LOGGER == ERROR
            LOGGER.fatal("Memory dump to file disabled");
            // #endif /* LOGGER == ERROR */
        }
    }

    /**
     * Status object for the memory component containing various information
     * about it.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
     */
    public static class Status implements Importable, Exportable {
        private StorageUnit m_freeMemory;
        private StorageUnit m_maxChunkSize;
        private StorageUnit m_totalMemory;
        private StorageUnit m_totalPayloadMemory;
        private long m_numberOfActiveMemoryBlocks = -1;
        private long m_numberOfActiveChunks = -1;
        private StorageUnit m_totalChunkPayloadMemory;
        private long m_cidTableCount = -1;
        private StorageUnit m_totalMemoryCIDTables;
        private int m_cachedFreeLIDs = -1;
        private long m_availableFreeLIDs = -1;
        private long m_newLIDCounter = -1;

        /**
         * Default constructor
         */
        public Status() {

        }

        /**
         * Get the amount of free memory
         *
         * @return Free memory
         */
        public StorageUnit getFreeMemory() {
            return m_freeMemory;
        }

        /**
         * Get the max allowed chunk size
         *
         * @return Max chunk size
         */
        public StorageUnit getMaxChunkSize() {
            return m_maxChunkSize;
        }

        /**
         * Get the total amount of memory available
         *
         * @return Total amount of memory
         */
        public StorageUnit getTotalMemory() {
            return m_totalMemory;
        }

        /**
         * Get the total number of active/allocated memory blocks
         *
         * @return Number of allocated memory blocks
         */
        public long getNumberOfActiveMemoryBlocks() {
            return m_numberOfActiveMemoryBlocks;
        }

        /**
         * Get the total number of currently active chunks
         *
         * @return Number of active/allocated chunks
         */
        public long getNumberOfActiveChunks() {
            return m_numberOfActiveChunks;
        }

        /**
         * Get the amount of memory used by chunk payload/data
         *
         * @return Amount of memory used by chunk payload
         */
        public StorageUnit getTotalChunkPayloadMemory() {
            return m_totalChunkPayloadMemory;
        }

        /**
         * Get the number of currently allocated CID tables
         *
         * @return Number of CID tables
         */
        public long getCIDTableCount() {
            return m_cidTableCount;
        }

        /**
         * Get the total memory used by CID tables (payload only)
         *
         * @return Total memory used by CID tables
         */
        public StorageUnit getTotalMemoryCIDTables() {
            return m_totalMemoryCIDTables;
        }

        /**
         * Get the total amount of memory allocated and usable for actual payload/data
         *
         * @return Total amount of memory usable for payload
         */
        public StorageUnit getTotalPayloadMemory() {
            return m_totalPayloadMemory;
        }

        /**
         * Get the number of cached LIDs in the LID store
         *
         * @return Number of cached LIDs
         */
        public int getCachedFreeLIDs() {
            return m_cachedFreeLIDs;
        }

        /**
         * Get the number of total available free LIDs of the LIDStore
         *
         * @return Total number of available free LIDs
         */
        public long getAvailableFreeLIDs() {
            return m_availableFreeLIDs;
        }

        /**
         * Get the current state of the counter generating new LIDs
         *
         * @return LID counter state
         */
        public long getNewLIDCounter() {
            return m_newLIDCounter;
        }

        @Override
        public int sizeofObject() {
            return Long.BYTES * 3 + m_freeMemory.sizeofObject() + m_totalMemory.sizeofObject() +
                    m_totalPayloadMemory.sizeofObject() + m_totalChunkPayloadMemory.sizeofObject() +
                    m_totalMemoryCIDTables.sizeofObject() + Integer.BYTES + Long.BYTES * 2;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.exportObject(m_freeMemory);
            p_exporter.exportObject(m_totalMemory);
            p_exporter.exportObject(m_totalPayloadMemory);
            p_exporter.writeLong(m_numberOfActiveMemoryBlocks);
            p_exporter.writeLong(m_numberOfActiveChunks);
            p_exporter.exportObject(m_totalChunkPayloadMemory);
            p_exporter.writeLong(m_cidTableCount);
            p_exporter.exportObject(m_totalMemoryCIDTables);
            p_exporter.writeInt(m_cachedFreeLIDs);
            p_exporter.writeLong(m_availableFreeLIDs);
            p_exporter.writeLong(m_newLIDCounter);
        }

        @Override
        public void importObject(final Importer p_importer) {
            if (m_freeMemory == null) {
                m_freeMemory = new StorageUnit();
            }
            p_importer.importObject(m_freeMemory);

            if (m_totalMemory == null) {
                m_totalMemory = new StorageUnit();
            }
            p_importer.importObject(m_totalMemory);

            if (m_totalPayloadMemory == null) {
                m_totalPayloadMemory = new StorageUnit();
            }
            p_importer.importObject(m_totalPayloadMemory);

            m_numberOfActiveMemoryBlocks = p_importer.readLong(m_numberOfActiveMemoryBlocks);
            m_numberOfActiveChunks = p_importer.readLong(m_numberOfActiveChunks);

            if (m_totalChunkPayloadMemory == null) {
                m_totalChunkPayloadMemory = new StorageUnit();
            }
            p_importer.importObject(m_totalChunkPayloadMemory);

            m_cidTableCount = p_importer.readLong(m_cidTableCount);

            if (m_totalMemoryCIDTables == null) {
                m_totalMemoryCIDTables = new StorageUnit();
            }
            p_importer.importObject(m_totalMemoryCIDTables);

            m_cachedFreeLIDs = p_importer.readInt(m_cachedFreeLIDs);
            m_availableFreeLIDs = p_importer.readLong(m_availableFreeLIDs);
            m_newLIDCounter = p_importer.readLong(m_newLIDCounter);
        }

        @Override
        public String toString() {
            String str = "";

            str += "Free memory: " + m_freeMemory.getHumanReadable() + " (" + m_freeMemory.getBytes() + ")\n";
            str += "Total memory: " + m_totalMemory.getHumanReadable() + " (" + m_totalMemory.getBytes() + ")\n";
            str += "Total payload memory: " + m_totalPayloadMemory.getHumanReadable() + " (" +
                    m_totalPayloadMemory.getBytes() + ")\n";
            str += "Num active memory blocks: " + m_numberOfActiveMemoryBlocks + '\n';
            str += "Num active chunks: " + m_numberOfActiveChunks + '\n';
            str += "Total chunk payload memory: " + m_totalChunkPayloadMemory.getHumanReadable() + " (" +
                    m_totalChunkPayloadMemory.getBytes() + ")\n";
            str += "Num CID tables: " + m_cidTableCount + '\n';
            str += "Total CID tables memory: " + m_totalMemoryCIDTables.getHumanReadable() + " (" +
                    m_totalChunkPayloadMemory.getBytes() + ")\n";
            str += "Num of free LIDs cached in LIDStore: " + m_cachedFreeLIDs + '\n';
            str += "Num of total available free LIDs in LIDStore: " + m_availableFreeLIDs + '\n';
            str += "New LID counter state: " + m_newLIDCounter;
            return str;
        }
    }
}
