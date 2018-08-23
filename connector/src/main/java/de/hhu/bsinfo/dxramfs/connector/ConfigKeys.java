package de.hhu.bsinfo.dxramfs.connector;

// Add dxramfs-annotations-2.0.0-cdh4.0.1.jar to the classpath

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfigKeys extends CommonConfigurationKeys {
    // you can (some of this are) overwrite THESE IMPORTANT DEFAULTS with etc/hadoop/core-site.xml
    public static final String BLOCK_SIZE_KEY = "dxram.file_blocksize";
    public static final long BLOCK_SIZE_DEFAULT = 4194304; // 4*1024*1024;
    public static final String  REPLICATION_KEY = "dxram.replication";
    public static final short REPLICATION_DEFAULT = 1;
    public static final String STREAM_BUFFER_SIZE_KEY = "dxram.stream-buffer-size";
    public static final int STREAM_BUFFER_SIZE_DEFAULT = 4096;
    public static final String BYTES_PER_CHECKSUM_KEY = "dxram.bytes-per-checksum";
    public static final int BYTES_PER_CHECKSUM_DEFAULT = 512;
    public static final String CLIENT_WRITE_PACKET_SIZE_KEY = "dxram.client-write-packet-size";
    public static final int CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
    public static final boolean ENCRYPT_DATA_TRANSFER_DEFAULT = false;
    public static final long FS_TRASH_INTERVAL_DEFAULT = 0;
    public static final DataChecksum.Type CHECKSUM_TYPE_DEFAULT = DataChecksum.Type.CRC32;
    public static final String KEY_PROVIDER_URI_DEFAULT = "";

    public static FsServerDefaults getServerDefaults() throws IOException {
        return new FsServerDefaults(
            BLOCK_SIZE_DEFAULT,
            BYTES_PER_CHECKSUM_DEFAULT,
            CLIENT_WRITE_PACKET_SIZE_DEFAULT,
            REPLICATION_DEFAULT,
            STREAM_BUFFER_SIZE_DEFAULT,
            ENCRYPT_DATA_TRANSFER_DEFAULT,
            FS_TRASH_INTERVAL_DEFAULT,
            CHECKSUM_TYPE_DEFAULT,
            KEY_PROVIDER_URI_DEFAULT);
    }
}
