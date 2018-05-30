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

package de.hhu.bsinfo.dxram.lookup;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.InvalidNodeRoleException;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NameserviceCacheEntryUpdateEvent;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayPeer;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlaySuperpeer;
import de.hhu.bsinfo.dxram.lookup.overlay.cache.CacheTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.NameserviceEntry;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponentConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.sync.SynchronizationServiceConfig;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageServiceConfig;
import de.hhu.bsinfo.dxram.util.ArrayListLong;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.Cache;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Component for finding chunks in superpeer overlay.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class LookupComponent extends AbstractDXRAMComponent<LookupComponentConfig> implements EventListener<AbstractEvent> {
    private static final short ORDER = 10;

    // component dependencies
    private BackupComponent m_backup;
    private AbstractBootComponent m_boot;
    private EventComponent m_event;
    private NetworkComponent m_network;

    private OverlaySuperpeer m_superpeer;
    private OverlayPeer m_peer;

    private CacheTree m_chunkIDCacheTree;
    private Cache<Integer, Long> m_applicationIDCache;

    /**
     * Creates the lookup component
     */
    public LookupComponent() {
        super(DXRAMComponentOrder.Init.LOOKUP, DXRAMComponentOrder.Shutdown.LOOKUP, LookupComponentConfig.class);
    }

    /**
     * Get the number of entries in name service
     *
     * @return the number of name service entries
     */
    public int getNameserviceEntryCount() {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.getNameserviceEntryCount();
    }

    /**
     * Get all available nameservice entries.
     *
     * @return List of nameservice entries or null on error.
     */
    public ArrayList<NameserviceEntry> getNameserviceEntries() {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.getNameserviceEntries();
    }

    /**
     * Checks if all superpeers are offline
     *
     * @return if all superpeers are offline
     */
    public boolean isResponsibleForBootstrapCleanup() {
        boolean ret;

        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            ret = m_superpeer.isLastSuperpeer();
        } else {
            ret = m_peer.allSuperpeersDown();
        }

        return ret;
    }

    /**
     * Get the corresponding LookupRange for the given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the current location and the range borders
     */
    public LookupRange getLookupRange(final long p_chunkID) {
        LookupRange ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering getLookupRange with: p_chunkID=0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            // Read from cache
            ret = m_chunkIDCacheTree.getMetadata(p_chunkID);
            if (ret == null) {
                // Cache miss -> get LookupRange from superpeer
                ret = m_peer.getLookupRange(p_chunkID);

                // Add response to cache
                if (ret != null && ret.getState() == LookupState.OK) {
                    m_chunkIDCacheTree.cacheRange(((long) ChunkID.getCreatorID(p_chunkID) << 48) + ret.getRange()[0],
                            ((long) ChunkID.getCreatorID(p_chunkID) << 48) + ret.getRange()[1], ret.getPrimaryPeer());
                }
            }
        } else {
            ret = m_peer.getLookupRange(p_chunkID);
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting getLookupRange");
        // #endif /* LOGGER == TRACE */
        return ret;
    }

    /**
     * Remove the ChunkIDs from range after deletion of that chunks
     *
     * @param p_chunkIDs
     *         the ChunkIDs
     */
    public void removeChunkIDs(final ArrayListLong p_chunkIDs) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering remove with %d chunkIDs", p_chunkIDs.getSize());
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            invalidate(p_chunkIDs);
        }

        m_peer.removeChunkIDs(p_chunkIDs);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting remove");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Insert a new name service entry
     *
     * @param p_id
     *         the AID
     * @param p_chunkID
     *         the ChunkID
     */
    public void insertNameserviceEntry(final int p_id, final long p_chunkID) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // Insert ChunkID <-> ApplicationID mapping
        // #if LOGGER == TRACE
        LOGGER.trace("Entering insertID with: p_id=%d, p_chunkID=0x%X", p_id, p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            m_applicationIDCache.put(p_id, p_chunkID);
        }

        m_peer.insertNameserviceEntry(p_id, p_chunkID);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting insertID");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Get ChunkID for give AID
     *
     * @param p_id
     *         the AID
     * @param p_timeoutMs
     *         Timeout for trying to get the entry (if it does not exist, yet).
     *         set this to -1 for infinite loop if you know for sure, that the entry has to exist
     * @return the corresponding ChunkID
     */
    public long getChunkIDForNameserviceEntry(final int p_id, final int p_timeoutMs) {
        long ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
        // #if LOGGER == TRACE
        LOGGER.trace("Entering getChunkID with: p_id=%d", p_id);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            // Read from application cache first
            final Long chunkID = m_applicationIDCache.get(p_id);

            if (chunkID == null) {
                // Cache miss -> ask superpeer
                // #if LOGGER == TRACE
                LOGGER.trace("Value not cached for application cache: %d", p_id);
                // #endif /* LOGGER == TRACE */

                ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);

                // Cache response
                m_applicationIDCache.put(p_id, ret);
            } else {
                ret = chunkID;
            }
        } else {
            ret = m_peer.getChunkIDForNameserviceEntry(p_id, p_timeoutMs);
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting getChunkID");
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Store migration of given ChunkID to a new location
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_nodeID
     *         the new owner
     */
    public void migrate(final long p_chunkID, final short p_nodeID) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering migrate with: p_chunkID=0x%X, p_nodeID=0x%X", p_chunkID, p_nodeID);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            invalidate(p_chunkID);
        }

        m_peer.migrate(p_chunkID, p_nodeID);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting migrate");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Store migration of a range of ChunkIDs to a new location
     *
     * @param p_startCID
     *         the first ChunkID
     * @param p_endCID
     *         the last ChunkID
     * @param p_nodeID
     *         the new owner
     */
    public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering migrateRange with: p_startChunkID=0x%X, p_endChunkID=0x%X, p_nodeID=0x%X", p_startCID, p_endCID, p_nodeID);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            invalidate(p_startCID, p_endCID);
        }

        m_peer.migrateRange(p_startCID, p_endCID, p_nodeID);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting migrateRange");
        // #endif /* LOGGER == TRACE */
    }

    //

    /**
     * Initialize a new backup range
     *
     * @param p_backupRange
     *         the backup range to initialize
     */
    public void initRange(final BackupRange p_backupRange) {

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering initRange with: p_backupRange=%s", p_backupRange);
        // #endif /* LOGGER == TRACE */

        m_peer.initRange(p_backupRange);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting initRange");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Get all backup ranges for given node
     *
     * @param p_nodeID
     *         the NodeID
     * @return all backup ranges for given node
     */
    public BackupRange[] getAllBackupRanges(final short p_nodeID) {
        BackupRange[] ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering getAllBackupRanges with: p_nodeID=0x%X", p_nodeID);
        // #endif /* LOGGER == TRACE */

        ret = m_peer.getAllBackupRanges(p_nodeID);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting getAllBackupRanges");
        // #endif /* LOGGER == TRACE */
        return ret;
    }

    /**
     * Removes failed node from superpeer overlay
     *
     * @param p_failedNode
     *         NodeID of failed node
     * @param p_role
     *         NodeRole of failed node
     * @return whether this superpeer is responsible for the failed node
     */
    public boolean superpeersNodeFailureHandling(final short p_failedNode, final NodeRole p_role) {
        return m_superpeer.nodeFailureHandling(p_failedNode, p_role);
    }

    /**
     * Invalidates the cache entry for given ChunkIDs
     *
     * @param p_chunkIDs
     *         the IDs
     */
    public void invalidate(final long... p_chunkIDs) {
        if (getConfig().cachesEnabled()) {
            for (long chunkID : p_chunkIDs) {
                assert chunkID != ChunkID.INVALID_ID;
                m_chunkIDCacheTree.invalidateChunkID(chunkID);
            }
        }
    }

    /**
     * Invalidates the cache entry for given ChunkIDs
     *
     * @param p_chunkIDs
     *         the IDs in an array list
     */
    public void invalidate(final ArrayListLong p_chunkIDs) {
        if (getConfig().cachesEnabled()) {
            for (int i = 0; i < p_chunkIDs.getSize(); i++) {
                assert p_chunkIDs.get(i) != ChunkID.INVALID_ID;
                m_chunkIDCacheTree.invalidateChunkID(p_chunkIDs.get(i));
            }
        }
    }

    /**
     * Invalidates the cache range for given ChunkID
     *
     * @param p_chunkID
     *         the ID whose range range is invalidated
     */
    public void invalidateRange(final long p_chunkID) {
        if (getConfig().cachesEnabled()) {
            assert p_chunkID != ChunkID.INVALID_ID;
            m_chunkIDCacheTree.invalidateRange(p_chunkID);
        }
    }

    /**
     * Allocate a barrier for synchronizing multiple peers.
     *
     * @param p_size
     *         Size of the barrier, i.e. number of peers that have to sign on until the barrier gets released.
     * @return Barrier identifier on success, -1 on failure.
     */
    public int barrierAllocate(final int p_size) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierAllocate(p_size);
    }

    /**
     * Free an allocated barrier.
     *
     * @param p_barrierId
     *         Barrier to free.
     * @return True if successful, false otherwise.
     */
    public boolean barrierFree(final int p_barrierId) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierFree(p_barrierId);
    }

    /**
     * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
     *
     * @param p_barrierId
     *         Id of an allocated barrier to change the size of.
     * @param p_newSize
     *         New size for the barrier.
     * @return True if changing size was successful, false otherwise.
     */
    public boolean barrierChangeSize(final int p_barrierId, final int p_newSize) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierChangeSize(p_barrierId, p_newSize);
    }

    /**
     * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
     *
     * @param p_barrierId
     *         Id of the barrier to sign on to.
     * @param p_customData
     *         Custom data to pass along with the sign on
     * @param p_waitForRelease
     *         True to wait for the barrier to be released, false to just sign on and don't wait for release (e.g. signal for remotes)
     * @return A pair consisting of the list of signed on peers and their custom data passed along with the sign ons, null on error
     */
    public BarrierStatus barrierSignOn(final int p_barrierId, final long p_customData, final boolean p_waitForRelease) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierSignOn(p_barrierId, p_customData, p_waitForRelease);
    }

    /**
     * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
     *
     * @param p_barrierId
     *         Id of the barrier to sign on to.
     * @param p_customData
     *         Custom data to pass along with the sign on
     * @return A pair consisting of the list of signed on peers and their custom data passed along with the sign ons, null on error
     */
    public BarrierStatus barrierSignOn(final int p_barrierId, final long p_customData) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierSignOn(p_barrierId, p_customData, true);
    }

    /**
     * Get the status of a specific barrier.
     *
     * @param p_barrierId
     *         Id of the barrier.
     * @return BarrierStatus or null if barrier does not exist
     */
    public BarrierStatus barrierGetStatus(final int p_barrierId) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.barrierGetStatus(p_barrierId);
    }

    /**
     * Create a block of memory in the superpeer storage.
     *
     * @param p_storageId
     *         Storage id to use to identify the block.
     * @param p_size
     *         Size of the block to allocate
     * @return True if successful, false on failure (no space, element count exceeded or id used).
     */
    public boolean superpeerStorageCreate(final int p_storageId, final int p_size) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.superpeerStorageCreate(p_storageId, p_size);
    }

    /**
     * Create a block of memory in the superpeer storage.
     *
     * @param p_dataStructure
     *         Data structure with the storage id assigned to allocate memory for.
     * @return True if successful, false on failure (no space, element count exceeded or id used).
     */
    public boolean superpeerStorageCreate(final DataStructure p_dataStructure) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for data struct to allocate memory in superpeer storage", p_dataStructure.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return superpeerStorageCreate((int) p_dataStructure.getID(), p_dataStructure.sizeofObject());
    }

    /**
     * Put data into an allocated block of memory in the superpeer storage.
     *
     * @param p_dataStructure
     *         Data structure to put with the storage id assigned.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStoragePut(final DataStructure p_dataStructure) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for data struct to put data into superpeer storage", p_dataStructure.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return m_peer.superpeerStoragePut(p_dataStructure);
    }

    /**
     * Put data into an allocated block of memory in the superpeer storage (anonymous chunk)
     *
     * @param p_chunk
     *         Chunk to put with the storage id assigned.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStoragePutAnon(final ChunkAnon p_chunk) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_chunk.getID() > 0x7FFFFFFF || p_chunk.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for anonymous chunk to put data into superpeer storage", p_chunk.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return m_peer.superpeerStoragePutAnon(p_chunk);
    }

    /**
     * Get data from the superpeer storage.
     *
     * @param p_dataStructure
     *         Data structure with the storage id assigned to read the data into.
     * @return True on success, false on failure.
     */
    public boolean superpeerStorageGet(final DataStructure p_dataStructure) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for data struct to get data from superpeer storage", p_dataStructure.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return m_peer.superpeerStorageGet(p_dataStructure);
    }

    /**
     * Get data from the superpeer storage (anonymous chunk)
     *
     * @param p_chunk
     *         Reference to anonymous chunk with the storage id assigned to read the data into.
     * @return True on success, false on failure.
     */
    public boolean superpeerStorageGetAnon(final ChunkAnon p_chunk) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_chunk.getID() > 0x7FFFFFFF || p_chunk.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for anonymous chunk to get data from superpeer storage", p_chunk.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return m_peer.superpeerStorageGetAnon(p_chunk);
    }

    /**
     * Remove an allocated block from the superpeer storage.
     *
     * @param p_id
     *         Storage id identifying the block to remove.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStorageRemove(final int p_id) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        m_peer.superpeerStorageRemove(p_id);
        return true;
    }

    /**
     * Remove an allocated block from the superpeer storage.
     *
     * @param p_dataStructure
     *         Data structure with the storage id assigned to remove.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStorageRemove(final DataStructure p_dataStructure) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructure.getID() > 0x7FFFFFFF || p_dataStructure.getID() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid id 0x%X for data struct to remove data from superpeer storage", p_dataStructure.getID());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        m_peer.superpeerStorageRemove((int) p_dataStructure.getID());
        return true;
    }

    /**
     * Get the status of the superpeer storage.
     *
     * @return Status of the superpeer storage.
     */
    public SuperpeerStorage.Status superpeerStorageGetStatus() {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.superpeerStorageGetStatus();
    }

    /**
     * Replaces the backup peer for given range on responsible superpeer
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_failedPeer
     *         the failed peer
     * @param p_newPeer
     *         the replacement
     */
    public void replaceBackupPeer(final short p_rangeID, final short p_failedPeer, final short p_newPeer) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        m_peer.replaceBackupPeer(p_rangeID, p_failedPeer, p_newPeer);
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent) {

            NodeFailureEvent event = (NodeFailureEvent) p_event;

            if (event.getRole() == NodeRole.PEER) {
                if (getConfig().cachesEnabled()) {
                    m_chunkIDCacheTree.invalidatePeer(event.getNodeID());
                }
            }

        } else if (p_event instanceof NameserviceCacheEntryUpdateEvent) {

            NameserviceCacheEntryUpdateEvent event = (NameserviceCacheEntryUpdateEvent) p_event;
            // update if available to avoid caching all entries
            if (m_applicationIDCache.contains(event.getId())) {
                m_applicationIDCache.put(event.getId(), event.getChunkID());
            }
        }
    }

    @Override
    public boolean finishInitComponent() {

        if (m_boot.getNodeRole() == NodeRole.PEER) {
            InetSocketAddress socketAddress = m_boot.getNodeAddress(m_boot.getNodeID());
            m_peer.finishStartup(m_boot.getRack(), m_boot.getSwitch(), m_backup.isActiveAndAvailableForBackup(),
                    new IPV4Unit(socketAddress.getHostString(), socketAddress.getPort()));
        }

        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        // Set static values for backup range (cannot be set in BackupComponent as superpeers do not initialize it)
        BackupRange.setReplicationFactor(p_config.getComponentConfig(BackupComponentConfig.class).getReplicationFactor());
        BackupRange.setBackupRangeSize(p_config.getComponentConfig(BackupComponentConfig.class).getBackupRangeSize().getBytes());

        if (getConfig().cachesEnabled()) {
            m_chunkIDCacheTree = new CacheTree(ORDER, getConfig().getCacheTtl().getMs(), getConfig().getMaxCacheEntries());

            // TODO: Check cache! If number of entries is smaller than number of entries in nameservice, bg won't terminate.
            m_applicationIDCache = new Cache<>(p_config.getComponentConfig(NameserviceComponentConfig.class).getNameserviceCacheEntries());
            // m_aidCache.enableTTL();

            m_event.registerListener(this, NodeFailureEvent.class);
        }

        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            m_superpeer = new OverlaySuperpeer(m_boot.getNodeID(), m_boot.getNodeIDBootstrap(), m_boot.getNumberOfAvailableSuperpeers(),
                    (int) getConfig().getStabilizationBreakTime(), p_config.getServiceConfig(SynchronizationServiceConfig.class).getMaxBarriersPerSuperpeer(),
                    p_config.getServiceConfig(TemporaryStorageServiceConfig.class).getStorageMaxNumEntries(),
                    (int) p_config.getServiceConfig(TemporaryStorageServiceConfig.class).getStorageMaxSize().getBytes(),
                    p_config.getComponentConfig(BackupComponentConfig.class).isBackupActive(), m_boot, m_network, m_event);
        } else {
            m_peer = new OverlayPeer(m_boot.getNodeID(), m_boot.getNodeIDBootstrap(), m_boot.getNumberOfAvailableSuperpeers(), m_boot, m_network, m_event);
            m_event.registerListener(this, NameserviceCacheEntryUpdateEvent.class);
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_superpeer != null) {
            m_superpeer.shutdown();
        }

        if (getConfig().cachesEnabled()) {
            if (m_chunkIDCacheTree != null) {
                m_chunkIDCacheTree.close();
                m_chunkIDCacheTree = null;
            }
            if (m_applicationIDCache != null) {
                m_applicationIDCache.clear();
                m_applicationIDCache = null;
            }
        }

        return true;
    }

    /**
     * Returns all known superpeers
     *
     * @return array with all superpeers
     */
    ArrayList<Short> getAllSuperpeers() {
        return m_peer.getAllSuperpeers();
    }

    /**
     * Get the corresponding primary peer (the peer storing the Chunk in RAM) for the given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the primary peer
     */
    short getPrimaryPeer(final long p_chunkID) {
        short ret = NodeID.INVALID_ID;
        LookupRange lookupRange;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("Entering getPrimaryPeer with: p_chunkID=0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (getConfig().cachesEnabled()) {
            // Read from cache
            ret = m_chunkIDCacheTree.getPrimaryPeer(p_chunkID);
            if (ret == NodeID.INVALID_ID) {
                // Cache miss -> get LookupRange from superpeer
                lookupRange = m_peer.getLookupRange(p_chunkID);

                // Add response to cache
                if (lookupRange != null) {
                    m_chunkIDCacheTree.cacheRange(((long) ChunkID.getCreatorID(p_chunkID) << 48) + lookupRange.getRange()[0],
                            ((long) ChunkID.getCreatorID(p_chunkID) << 48) + lookupRange.getRange()[1], lookupRange.getPrimaryPeer());

                    ret = lookupRange.getPrimaryPeer();
                }
            }
        } else {
            lookupRange = m_peer.getLookupRange(p_chunkID);
            if (lookupRange != null) {
                ret = lookupRange.getPrimaryPeer();
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting getPrimaryPeer");
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    // --------------------------------------------------------------------------------

    /**
     * Returns the responsible superpeer for given peer
     *
     * @param p_nodeID
     *         the peer
     * @return the responsible superpeer
     */
    public short getResponsibleSuperpeer(final short p_nodeID) {
        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        return m_peer.getResponsibleSuperpeer(p_nodeID);
    }

    /**
     * Get Lookup Tree from Superpeer
     *
     * @param p_nodeID
     *         the NodeID
     * @return LookupTree from SuperPeerOverlay
     * @note This method must be called by a superpeer
     */
    LookupTree superPeerGetLookUpTree(final short p_nodeID) {
        LookupTree ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() != NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER >= TRACE
        LOGGER.trace("Entering getSuperPeerLookUpTree with: p_nodeID=0x%X", p_nodeID);
        // #endif /* LOGGER >= TRACE */

        ret = m_superpeer.getLookupTree(p_nodeID);

        LOGGER.trace("Exiting getSuperPeerLookUpTree");
        return ret;
    }

    /**
     * Invalidates the cache entry for given ChunkID range
     *
     * @param p_startCID
     *         the first ChunkID
     * @param p_endCID
     *         the last ChunkID
     */
    private void invalidate(final long p_startCID, final long p_endCID) {
        long iter = p_startCID;
        while (iter <= p_endCID) {
            invalidate(iter++);
        }
    }

    /**
     * Clear the cache
     */
    @SuppressWarnings("unused")
    private void clear() {
        if (getConfig().cachesEnabled()) {
            m_chunkIDCacheTree = new CacheTree(ORDER, getConfig().getCacheTtl().getMs(), getConfig().getMaxCacheEntries());
            m_applicationIDCache.clear();
        }
    }
}
