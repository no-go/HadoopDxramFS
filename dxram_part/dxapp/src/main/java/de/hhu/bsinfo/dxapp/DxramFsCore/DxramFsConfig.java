package de.hhu.bsinfo.dxapp.dxramfscore;
import java.nio.charset.Charset;

public class DxramFsConfig {
    // @todo: US_ASCII is fix in DXRAM for string, but it should be the standard java+hadoop utf16 ! (4. Sep 2018)  
    public static final Charset STRING_STD_CHARSET = java.nio.charset.StandardCharsets.US_ASCII;
    
    public static final long INVALID_ID = -1;   // only important for the dxram part of the project

    public static String ROOT_Chunk;            // only important for the dxram part of the project
    public static String dxnet_to_dxram_peers;

    public static int file_blocksize;
    public static int ref_ids_each_fsnode;
    public static int max_pathlength_chars;

    public static int max_filenamelength_chars;
    public static int max_hostlength_chars;
    public static int max_addrlength_chars;
    
    public class GsonFiller {
        public String ROOT_Chunk;
        public String dxnet_to_dxram_peers;
        public int file_blocksize;
        public int ref_ids_each_fsnode;
        public int max_pathlength_chars;
        public int max_filenamelength_chars;
        public int max_hostlength_chars;
        public int max_addrlength_chars;
    }
};
