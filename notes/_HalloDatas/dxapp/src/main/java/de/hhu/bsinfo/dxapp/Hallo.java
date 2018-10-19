package de.hhu.bsinfo.dxapp;
import java.nio.charset.StandardCharsets;

public class Hallo {
    public byte[] _data;
    public String _host;
    public int _port;
     
    public Hallo() {
        _data = new byte[0];
        _host = "              a    ";
        _port = 4;
    }
    
    public Hallo(int size) {
        _data = new byte[size];
        _host = "             b     ";
        _port = 123;
    }
    
    // copies only allowed size of bytes into data from a given string
    public void setData(String str) {
        byte[] src = str.getBytes(StandardCharsets.US_ASCII);
        for(int i = 0; i<_data.length; i++) {
            if (i < src.length) {
                _data[i] = src[i];
            } else {
                i=_data.length; // finish
            }
        }
    }
}
