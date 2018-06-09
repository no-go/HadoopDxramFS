package de.hhu.bsinfo.dxramfs.connector;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxramfs.core.DxnetInit;
import de.hhu.bsinfo.dxramfs.core.NodePeerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DxramFileSystem extends FileSystem {
    public static final Logger LOG = LogManager.getLogger(DxramFileSystem.class.getName());

    private static final Path ROOT_PATH = new Path(Path.SEPARATOR);
    private static final String SCHEME = "dxram";

    private URI _myUri;
    private Path _workingDir;
    private DXNet _dxn;
    public static NodePeerConfig nopeConfig;

    @Override
    public URI getUri() {
        return _myUri;
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"()");
        _workingDir = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"()");
        return _workingDir;
    }

    @Override
//    protected Path fixRelativePart(Path p) {
    public Path fixRelativePart(Path p) {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", p);
        return super.fixRelativePart(p);
    }

    private DXNet connect() {
        nopeConfig = new NodePeerConfig();
        nopeConfig.nodeId = Short.valueOf(getConf().get("dxnet_local_id"));
        nopeConfig.addr = getConf().get("dxnet_local_addr");
        nopeConfig.port = Integer.valueOf(getConf().get("dxnet_local_port"));

        nopeConfig.dxPeers = new ArrayList<>();

        // @todo if you realy want to run many dxram/dxnet peers on local, you have to change this !!
        NodePeerConfig.PeerConfig aPeer = new NodePeerConfig.PeerConfig();
        aPeer.nodeId = Short.valueOf(getConf().get("dxnet_local_peer_id"));
        aPeer.addr = getConf().get("dxnet_local_peer_addr");
        aPeer.port = Integer.valueOf(getConf().get("dxnet_local_peer_port"));
        nopeConfig.dxPeers.add(aPeer);

        DxnetInit dxini = new DxnetInit(nopeConfig, nopeConfig.nodeId);
        return dxini.getDxNet();
    }

    /**
     * Called after a new FileSystem instance is constructed.
     * @param theUri a uri whose authority section names the host, port, etc. for
     *          this FileSystem
     * @param conf the configuration
     */
    @Override
    public void initialize(final URI theUri, final Configuration conf)
        throws IOException {
        super.initialize(theUri, conf);
        setConf(conf);
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", theUri, conf);
        String authority = theUri.getAuthority();
        _dxn = connect();
        try {
            _myUri = new URI(SCHEME, authority, "/", null, null);
            LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+" myuri: {}", _myUri);
            _workingDir = this.getHomeDirectory();
            LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+" working Dir: {}", _workingDir);

        } catch (URISyntaxException e) {
            throw new IOException("URISyntax exception: " + theUri);
        }
    }

    /**
     * this is a dummy
     */
    @Override
    public FSDataOutputStream append(
        Path f, int bufferSize, Progressable progress
    ) throws IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {}, {})",
            f, bufferSize, progress);
        return null;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", f, bufferSize);
        Path absF = fixRelativePart(f);
        long blocksize = getServerDefaults(absF).getBlockSize();
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blocksize);
        return dxfile.open(bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", src, dst);
        
        Path absF1 = fixRelativePart(src);
        Path absF2 = fixRelativePart(dst);
        long blocksize = getServerDefaults(absF1).getBlockSize();
        long blocksize2 = getServerDefaults(absF2).getBlockSize();

        DxramFile file = new DxramFile(_dxn, absF1, _myUri, blocksize);
        DxramFile file2 = new DxramFile(_dxn, absF2, _myUri, blocksize2);
        
        if (file2.exists()) {
            throw new java.io.IOException("destination file exists");
        }
        if (!file.exists()) {
            throw new java.io.IOException("source file exists");
        }

        return file.renameTo(file2);
    }

    @Override
    public FSDataOutputStream create(
        Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress
    ) throws
        FileAlreadyExistsException,
        IOException 
    {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName() +
            "({}, {}, {}, {}, {}, {}, {})",
            f, permission, overwrite, bufferSize, replication, blockSize, progress);

        Path absF = fixRelativePart(f);
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blockSize);
        
        return dxfile.create(bufferSize, replication, true);
    }

    @Override
    public FSDataOutputStream createNonRecursive(
        Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress
    ) throws 
        FileAlreadyExistsException,
        IOException
    {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName() +
            "({}, {}, {}, {}, {}, {}, {})",
            f, permission, overwrite, bufferSize, replication, blockSize, progress);

        Path absF = fixRelativePart(f);
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blockSize);
        
        return dxfile.create(bufferSize, replication, false);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws FileAlreadyExistsException, IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", f, permission);
        Path absF = fixRelativePart(f);
        long blocksize = getServerDefaults(absF).getBlockSize();
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blocksize);
        if (dxfile.exists()) 
            throw new FileAlreadyExistsException("mkdirs: " + dxfile.toString() + " exists");
        return dxfile.mkdirs();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws FileNotFoundException, IOException {
        boolean isDel;
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", f, recursive);
        Path absF = fixRelativePart(f);
        long blocksize = getServerDefaults(absF).getBlockSize();
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blocksize);
        
        if (!dxfile.exists()) {
            if (recursive) {
                /**
                 * @todo right behavior? delete a not existing file recursive 
                 * (it is a directory?) is not a problem, is it?
                 */
                return true;
            }
            throw new FileNotFoundException("delete: " + f.toString() + " not exists"); 
        }
        
        if (dxfile.isDirectory()) {
            if (recursive) {
                isDel = delete(dxfile);
                if (isDel == true) return true;
            }
            return false;
        } else {
            isDel = delete(dxfile);
            if (isDel == true) {
                return true;
            } else {
                new IOException();
                return false;
            }
            
        }
    }

    private boolean delete(DxramFile file) throws IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", file);
        boolean isDel = false;
        boolean isFolder = file.isDirectory();
        if (isFolder) {
            for (DxramFile childFile : file.listFiles()) {
                isDel = delete(childFile);
                if (isDel == false) return false;
            }
        }
        return file.delete();
    }

    @Override
    public FileStatus[] listStatus(Path p) throws FileNotFoundException, IOException {

        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", p);

        /**
         * @todo hack: listStatus [] I get connector://abook.localhost.fake:9000/tmp/myfs
         *     and not listStatus [] connector://abook.localhost.fake:9000/
         */
        /*
        String check = p.toString() + "/";
        if (check.startsWith(_myUri.toString()) && check.endsWith(DEBUG_LOCAL)) {
            p = new Path(_myUri);
        }
        */
        
        ArrayList<FileStatus> statusArrayList = new ArrayList<>();
        FileStatus fileStatus = getFileStatus(fixRelativePart(p));

        if (fileStatus.isFile()) {
            statusArrayList.add(fileStatus);

        } else if (fileStatus.isDirectory()) {
            DxramFile file = new DxramFile(
                    _dxn,
                    fileStatus.getPath(),
                    _myUri,
                    fileStatus.getBlockSize()
            );
            for (DxramFile childFile : file.listFiles()) {
                statusArrayList.add(childFile.getFileStatus());
            }
        }

        return statusArrayList.toArray(new FileStatus[statusArrayList.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path p) throws FileNotFoundException, IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", p);
        
        Path absF = fixRelativePart(p);
        long blocksize = getServerDefaults(absF).getBlockSize();
        DxramFile dxfile = new DxramFile(_dxn, absF, _myUri, blocksize);
        
        if (!dxfile.exists()) {
            throw new FileNotFoundException("_getFileStatus: " + p.toString() + " not exists");
        }
        
        return dxfile.getFileStatus();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(
            Path f,
            long start,
            long len
    ) throws
            AccessControlException,
            FileNotFoundException,
            UnresolvedLinkException,
            IOException
    {
        Path abs = fixRelativePart(f);
        long blocksize = getServerDefaults(abs).getBlockSize();
        return new DxramFile(_dxn, abs, getUri(), blocksize).getFileBlockLocations(start, len);
    }

}
