package de.hhu.bsinfo.dxramfs.connector;

import de.hhu.bsinfo.dxnet.DXNet;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

import java.net.URI;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;

public class DxramFile {
    /// @todo File OP
    public static final String DEBUG_LOCAL = "/tmp/myfs/";
    
    public static final Logger LOG = LogManager.getLogger(DxramFile.class.getName());
    
    private Path        _absPath;
    private URI         _uri;
    private long        _blocksize;
    private DXNet       _dxnet;
    
    /// @todo File OP
    private java.io.File _dummy;

    public DxramFile(DXNet dxnet, Path absPath, URI uri) {
        _dxnet     = dxnet;
        _uri       = uri;
        _blocksize = DxramFsConfig.file_blocksize;
        _absPath   = absPath;
        
        /// @todo File OP
        String s = hpath2lDEBUGpath(_absPath);
        _dummy   = new java.io.File(s);
    }

    /** @todo File OP
     *  connector://localhost:9000/abc/de -> /tmp/myfs/abc/de
     */
    private String hpath2lDEBUGpath(Path hpath) {
        return hpath.toString().replace(_uri.toString(), DEBUG_LOCAL);
    }

    /**
     *  connector://localhost:9000/abc/de -> abc/de
     */
    private String hpath2path(Path hpath) {
        return hpath.toString().replace(_uri.toString(), "");
    }

    /** @todo File OP
     *  /tmp/myfs/abc/de -> connector://localhost:9000/abc/de
     */
    private Path lpath2hpath(String lpath) {
        Path p = new Path(lpath.replace(DEBUG_LOCAL, _uri.toString()));
        /**
         * @todo hack: bad   connector://abook.localhost.fake:9000/tmp/myfs
         *            good   connector://abook.localhost.fake:9000/
         */
        if ((lpath + Path.SEPARATOR).equals(DEBUG_LOCAL)) {
            p = new Path(_uri);
        }
        return p;
    }

    public Path getPath() {
        return _absPath;
    }









    public boolean exists() {
        ExistsMessage msg = new ExistsMessage(DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId, hpath2path(_absPath));
        boolean res = msg.send(_dxnet);
        LOG.debug("exists msg Response: " + String.valueOf(res));
        _dummy.exists();
        return res;
    }

    public boolean mkdirs() throws IOException {
        MkDirsMessage msg = new MkDirsMessage(DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId, hpath2path(_absPath));
        boolean res = msg.send(_dxnet);
        LOG.debug("mkdirs msg Response: " + String.valueOf(res));
        _dummy.mkdirs();
        return res;
    }


    public boolean isDirectory() {
        IsDirectoryMessage msg = new IsDirectoryMessage(DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId, hpath2path(_absPath));
        boolean res = msg.send(_dxnet);
        LOG.debug("isdir msg Response: " + String.valueOf(res));
        _dummy.isDirectory();
        return res;
    }
    
    public long length() {
        FileLengthMessage msg = new FileLengthMessage(DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId, hpath2path(_absPath));
        long res = msg.send(_dxnet);
        LOG.debug("length msg Response: " + String.valueOf(res));
        return _dummy.length();
    }

    public boolean delete() throws IOException {
        DeleteMessage msg = new DeleteMessage(DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId, hpath2path(_absPath));
        boolean res = msg.send(_dxnet);
        LOG.debug("delete msg Response: " + String.valueOf(res));
        return _dummy.delete();
    }
    
    public boolean renameTo(DxramFile dest) {
        RenameToMessage msg = new RenameToMessage(
                DxramFileSystem.nopeConfig.dxPeers.get(0).nodeId,
                hpath2path(_absPath),
                hpath2path(dest._absPath)
        );
        boolean res = msg.send(_dxnet);
        LOG.debug("renameTo msg Response: " + String.valueOf(res));
        return _dummy.renameTo(dest._dummy);
    }







    public DxramFile[] listFiles() throws FileNotFoundException, IOException {
        ArrayList<DxramFile> fArrayList = new ArrayList<>();
        //send("listFiles() " + _dummy.getName());

        /// @todo File OP
        for (File childFile : _dummy.listFiles()) {
            Path p = lpath2hpath(childFile.getAbsolutePath());
            
            /// @todo _blocksize is a dummy at this place, because local fs did not store it
            DxramFile dxfile = new DxramFile(_dxnet, p, _uri);
            
            fArrayList.add(dxfile);
        }
        return fArrayList.toArray(new DxramFile[fArrayList.size()]);
    }
    
    public FileStatus getFileStatus() {
        //send("getFileStatus() " + _dummy.getName());

        return new FileStatus(
            this.length(),
            this.isDirectory(),
            0, _blocksize, 0L, 0L, (FsPermission)null, (String)null, (String)null,
            getPath()
        );
    }

    //-------------------------------------------------------------------------------------- create
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
        //send("create: java io file.getAbsolutePath() " + _dummy.getName());
        String absolutePath = _dummy.getAbsolutePath();
        String filePath = absolutePath.substring(0, absolutePath.lastIndexOf(Path.SEPARATOR));
        
        //LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName() + "() filepath = {}",filePath);
        
        /// @todo File OP
        //send("create: java io Files.notExists(Paths.get(filePath))) " + filePath);
        if (Files.notExists(Paths.get(filePath))) {
            Path absF = lpath2hpath(filePath);
            DxramFile dxfile = new DxramFile(_dxnet, absF, _uri);
            dxfile.mkdirs();
        } else {
            if (recursive == false) throw new IOException("director(ies) do not exists");
        }
        
        /// @todo File OP
        //send("create: java io file.createNewFile() " + _dummy.getName());
        _dummy.createNewFile();
        OutputStream out = new FileOutputStream(_dummy);
        FSDataOutputStream outs = new FSDataOutputStream(out, (Statistics)null);
        return outs;
    }

    //-------------------------------------------------------------------------------------- open
    public FSDataInputStream open(int bufferSize) throws IOException {
        if (this.isDirectory()) throw new IOException("is directory");
        byte[] data = new byte[(int) this.length()];
        
        /// @todo File OP
        //send("open: java io FileInputStream() " + _dummy.getName());
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
         * -> but: where did the dxramfs ekosystem handle this?!
         */
        //send("getFileBlockLocations() " + _dummy.getName());

        return new BlockLocation[0];
    }

}
