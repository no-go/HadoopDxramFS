package de.hhu.bsinfo.app.dxramfscore;
import java.nio.charset.Charset;

public class DxramFsConfig {
    // @todo: US_ASCII is fix in DXRAM for string, but it should be the standard java+hadoop utf16 ! (4. Sep 2018)  
    private static final Charset STRING_STD_CHARSET = java.nio.charset.StandardCharsets.US_ASCII;

    public static int file_blocksize;
    public static int ref_ids_each_fsnode;
    public static int max_pathlength_chars;

    public static int max_filenamelength_chars;
    public static int max_hostlength_chars;
    public static int max_addrlength_chars;
};
