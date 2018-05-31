package de.hhu.bsinfo.hadoop.fs.dxram;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;
import de.hhu.bsinfo.hadoop.fs.dxnet.DxramFsPeer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import de.hhu.bsinfo.hadoop.fs.dxnet.A100bMessage;

import java.io.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DxramFileSystem extends FileSystem {
    public static final Logger LOG = LogManager.getLogger(DxramFileSystem.class.getName());

    private static final Path ROOT_PATH = new Path(Path.SEPARATOR);
    private static final String SCHEME = "dxram";

    private URI _myUri;
    private Path _workingDir;
    private DXNet dxnet;

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
        dxnet = DxramFsPeer.setup(getConf(), false);

        LOG.info("try to send something to dxnet Peer");

        dxnet.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );
        dxnet.register(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                new DxramFileSystem.InHandler()
        );

        A100bMessage msg = new A100bMessage(
                DxramFsPeer.NODEID_dxnet_peer,
                new String("Hallo Welt")
        );

        try {
            dxnet.sendMessage(msg);
        } catch (NetworkException e) {
            e.printStackTrace();
        }

        dxnet.close();


        String authority = theUri.getAuthority();
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
        DxramFile dxfile = new DxramFile(absF, _myUri, blocksize);
        return dxfile.open(bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", src, dst);
        
        Path absF1 = fixRelativePart(src);
        Path absF2 = fixRelativePart(dst);
        long blocksize = getServerDefaults(absF1).getBlockSize();
        long blocksize2 = getServerDefaults(absF2).getBlockSize();
        DxramFile file = new DxramFile(absF1, _myUri, blocksize);
        DxramFile file2 = new DxramFile(absF2, _myUri, blocksize2);
        
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
        DxramFile dxfile = new DxramFile(absF, _myUri, blockSize);
        
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
        DxramFile dxfile = new DxramFile(absF, _myUri, blockSize);
        
        return dxfile.create(bufferSize, replication, false);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws FileAlreadyExistsException, IOException {
        LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({}, {})", f, permission);
        Path absF = fixRelativePart(f);
        long blocksize = getServerDefaults(absF).getBlockSize();
        DxramFile dxfile = new DxramFile(absF, _myUri, blocksize);
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
        DxramFile dxfile = new DxramFile(absF, _myUri, blocksize);
        
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
         * @todo hack: listStatus [] I get dxram://abook.localhost.fake:9000/tmp/myfs
         *     and not listStatus [] dxram://abook.localhost.fake:9000/
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
        DxramFile dxfile = new DxramFile(absF, _myUri, blocksize);
        
        if (!dxfile.exists()) {
            throw new FileNotFoundException("_getFileStatus: " + p.toString() + " not exists");
        }
        
        return dxfile.getFileStatus();
    }

    // -------------------------xxxxxxxxxxxxxxxxxxxx-----------------xxxxxxxxxxxxxxxx------------

    public class InHandler implements MessageReceiver {
        @Override
        public void onIncomingMessage(Message p_message) {
            A100bMessage eMsg = (A100bMessage) p_message;
            LOG.info(Thread.currentThread().getStackTrace()[1].getMethodName()+"({})", eMsg.getData());
            //System.out.println(eMsg.toString());
        }
    }
}
