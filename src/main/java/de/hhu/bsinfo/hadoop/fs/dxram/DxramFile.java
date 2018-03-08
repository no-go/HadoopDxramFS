package de.hhu.bsinfo.hadoop.fs.dxram;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DxramFile {
    /// @todo File OP
    public static final String DEBUG_LOCAL = "/tmp/myfs/";
    
    public static final Logger LOG = LoggerFactory.getLogger(DxramFile.class);
    
    private Path         _absPath;
    private URI          _uri;
    private long         _blocksize;
    
    /// @todo File OP
    private java.io.File _dummy;
    
    public DxramFile(Path absPath, URI uri, long blocksize) {
        _uri       = uri;
        _blocksize = blocksize;
         _absPath  = absPath;
        
        /// @todo File OP
        String s = hpath2lpath(_absPath);
        _dummy   = new java.io.File(s);
    }
    
    /** @todo File OP
     *  dxram://localhost:9000/abc/de -> /tmp/myfs/abc/de
     */
    private String hpath2lpath(Path hpath) {
        return hpath.toString().replace(_uri.toString(), DEBUG_LOCAL);
    }
    
    /** @todo File OP
     *  /tmp/myfs/abc/de -> dxram://localhost:9000/abc/de
     */
    private Path lpath2hpath(String lpath) {
        Path p = new Path(lpath.replace(DEBUG_LOCAL, _uri.toString()));
        /**
         * @todo hack: bad   dxram://abook.localhost.fake:9000/tmp/myfs
         *            good   dxram://abook.localhost.fake:9000/
         */
        if ((lpath + Path.SEPARATOR).equals(DEBUG_LOCAL)) {
            p = new Path(_uri);
        }
        return p;
    }
    
    public boolean exists() {
        /// @todo File OP
        return _dummy.exists();
    }
    
    public boolean isDirectory() {
        /// @todo File OP
        return _dummy.isDirectory();
    }
    
    public long length() {
        /// @todo File OP
        return _dummy.length();
    }

    public boolean delete() throws IOException {
        /// @todo File OP
        return _dummy.delete();
    }
    
    public boolean renameTo(DxramFile dest) {
        /// @todo File OP
        return _dummy.renameTo(dest._dummy);
    }

    public boolean mkdirs() throws IOException {
        /// @todo File OP
        return _dummy.mkdirs();
    }
    
    public DxramFile[] listFiles() throws FileNotFoundException, IOException {
        ArrayList<DxramFile> fArrayList = new ArrayList<>();
        
        /// @todo File OP
        for (File childFile : _dummy.listFiles()) {
            Path p = lpath2hpath(childFile.getAbsolutePath());
            
            /// @todo _blocksize is a dummy at this place, because local fs did not store it
            DxramFile dxfile = new DxramFile(p, _uri, _blocksize);
            
            fArrayList.add(dxfile);
        }
        return fArrayList.toArray(new DxramFile[fArrayList.size()]);
    }
    
    public FileStatus getFileStatus() {
        return new FileStatus(
            this.length(),
            this.isDirectory(),
            0, _blocksize, 0L, 0L, (FsPermission)null, (String)null, (String)null,
            getPath()
        );
    }
    
    public Path getPath() {
        return _absPath;
    }
    
    public FSDataOutputStream create(
        int bufferSize, 
        short replication,
        boolean recursive
    ) throws
        FileAlreadyExistsException,
        IOException
    {
        if (this.exists() && this.isDirectory()) {
            throw new IOException("existing directory");
        }
        if (this.exists()) throw new FileAlreadyExistsException("file still exists");
        
        /// @todo File OP
        String absolutePath = _dummy.getAbsolutePath();
        String filePath = absolutePath.substring(0, absolutePath.lastIndexOf(Path.SEPARATOR));
        
        //LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName() + "() filepath = {}",filePath);
        
        /// @todo File OP
        if (Files.notExists(Paths.get(filePath))) {
            Path absF = lpath2hpath(filePath);
            DxramFile dxfile = new DxramFile(absF, _uri, _blocksize);
            dxfile.mkdirs();
        } else {
            if (recursive == false) throw new IOException("director(ies) do not exists");
        }
        
        /// @todo File OP
        _dummy.createNewFile();
        OutputStream out = new FileOutputStream(_dummy);
        FSDataOutputStream outs = new FSDataOutputStream(out, (Statistics)null);
        return outs;
    }
    
    public FSDataInputStream open(int bufferSize) throws IOException {
        if (this.isDirectory()) throw new IOException("is directory");
        byte[] data = new byte[(int) this.length()];
        
        /// @todo File OP
        FileInputStream fis = new FileInputStream(_dummy);
        fis.read(data);
        fis.close();
        
        DxramInputStream dxins = new DxramInputStream(data);
        FSDataInputStream dais = new FSDataInputStream(dxins);
        return dais;
    }

    public String toString() {
        return _absPath.toString();
    }
    
    
    public BlockLocation[] getFileBlockLocations(
        long start,
        long len
    ) throws
        AccessControlException, 
        FileNotFoundException, 
        UnresolvedLinkException,
        IOException
    {
        if (start < 0 || len < 0) throw new AccessControlException(
            "start block or length of block should not be negative"
        );
        
        /**
         * @todo: after implement a real distributed fs, we have to implement this !!!!
         * -> but: where did the hadoop ekosystem handle this?!
         */
        return new BlockLocation[0];
    }

}
