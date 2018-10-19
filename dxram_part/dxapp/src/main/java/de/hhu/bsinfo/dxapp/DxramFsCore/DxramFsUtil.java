package de.hhu.bsinfo.dxapp.dxramfscore;


public class DxramFsUtil {

    /**
     * Fills a byte array with a given size with the bytes of a string.
     * It uses DxramFsConfig.STRING_STD_CHARSET to convert.
     * This method will never fill out with more bytes than str needs
     *
     * @param out an allocated byte array
     * @param str the data we want to fill into out
     * @return the size of used bytes
     */
    public static int s2by(byte[] out, String str) {
        int i;
        int wantedSize = out.length;
        byte[] src = str.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        for(i = 0; i<wantedSize; i++) {
            if (i < src.length) {
                out[i] = src[i];
            } else {
                i=wantedSize; // finish
            }
        }
        return i;
    }
    
    public static String by2s(byte[] input, int size) {
        String out = new String(input, DxramFsConfig.STRING_STD_CHARSET);
        if (out.length() > size) {
            return out.substring(0, size);
        } else {
            return out;
        }
    }
}
