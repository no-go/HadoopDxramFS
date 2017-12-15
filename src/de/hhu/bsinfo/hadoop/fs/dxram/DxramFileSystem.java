package de.hhu.bsinfo.hadoop.fs.dxram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class DxramFileSystem extends FileSystem {

    private static final Path ROOT_PATH = new Path(Path.SEPARATOR);
    private static final String SCHEME = "dxram";
    private static final String DEBUG_LOCAL = "/tmp/";

    private URI _myUri;
    private Path _workingDir;

    private String _debugLocalWDir;

    @Override
    public URI getUri() {
        return _myUri;
    }

    @Override
    public String getScheme() {
        return SCHEME;
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

        System.out.printf("conf: %s\n", conf.toString());
        System.out.printf("theUri: %s\n", theUri.toString());

        String authority = theUri.getAuthority();
        try {
            _myUri = new URI(SCHEME, authority, "/", null, null);
            System.out.printf("myUri: %s\n", _myUri.toString());
            //_workingDir = this.getHomeDirectory();
            _workingDir = ROOT_PATH;
            System.out.printf("working Dir: %s\n", _workingDir.toString());

            _debugLocalWDir = _workingDir.toString().replace(
                _myUri.toString(),
                DEBUG_LOCAL
            ) + ROOT_PATH;

        } catch (URISyntaxException e) {
            throw new IOException("URISyntax exception: " + theUri);
        }
    }

        @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        File file = new File(_toLocal(f));

        if (getFileStatus(f).isDirectory()) throw new IOException("is directory");
        InputStream ins = new FileInputStream(file.getPath());
        FSDataInputStream is = new FSDataInputStream(ins);
        return is;
    }

    @Override
    public FSDataOutputStream create(
        Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress
    ) throws IOException {
        File file = new File(_toLocal(f));
        if (file.exists()) throw new IOException("file still exists");
        if (getFileStatus(f).isDirectory()) {
            throw new IOException("existing directory");
        }
        OutputStream out = new FileOutputStream(file);
        FSDataOutputStream outs = new FSDataOutputStream( out, (Statistics)null);
        return outs;
    }

    @Override
    public FSDataOutputStream append(
        Path f, int bufferSize, Progressable progress
    ) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        File file = new File(_toLocal(src));
        File file2 = new File(_toLocal(dst));

        if (file2.exists()) {
            throw new java.io.IOException("destination file exists");
        }
        if (!file.exists()) {
            throw new java.io.IOException("source file exists");
        }

        return file.renameTo(file2);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        File file = new File(_toLocal(f));
        if (getFileStatus(f).isDirectory()) {
            if (recursive) {
                _delete(file);
                if (!file.exists()) return true;
            }
            return false;
        }

        if (getFileStatus(f).isDirectory() && recursive == false) {
            return false;
        }

        if (file.exists() && getFileStatus(f).isFile() && recursive == false) {
            return file.delete();
        }

        return false;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        _workingDir = new_dir;
        _debugLocalWDir = DEBUG_LOCAL + new_dir.toString() + ROOT_PATH;
    }

    @Override
    public Path getWorkingDirectory() {
        return _workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        File file = new File(_toLocal(f));
        return file.mkdirs();
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        System.out.print("\nlistStatus [] " + f.toString() + "\n");

        ArrayList<FileStatus> statusArrayList = new ArrayList<>();
        FileStatus fileStatus = getFileStatus(f);

        if (!fileStatus.isDirectory()) {
            statusArrayList.add(fileStatus);

        } else {
            File file = new File(_toLocal(f));
            for (File childFile : file.listFiles()) {
                statusArrayList.add(_getFileStatus(childFile));
            }
        }

        return statusArrayList.toArray(new FileStatus[statusArrayList.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path p) throws IOException {
        System.out.print("\n" + p.toString() + "\n");
        //System.out.print("\n" + _toLocal(f) + "\n");

        File file = new File(_toLocal(p));
        return _getFileStatus(file);
    }

    public FileStatus _getFileStatus(File file) throws IOException {
        Path p = _fromLocal(file);
        long blocksize = getServerDefaults(p).getBlockSize();
        return new FileStatus(file.length(), file.isDirectory(), 0, blocksize, 0L, 0L, (FsPermission)null, (String)null, (String)null, p);
    }

    private static void _delete(File file) throws IOException {
        for (File childFile : file.listFiles()) {
            if (childFile.isDirectory()) {
                _delete(childFile);
            } else {
                if (!childFile.delete()) {
                    throw new IOException();
                }
            }
        }

        if (!file.delete()) {
            throw new IOException();
        }
    }

    private String _toLocal(Path p) {
        String s = p.toString().replace(_myUri.toString(), DEBUG_LOCAL);
        // String s2 = _workingDir.toString().replace(_myUri.toString(), DEBUG_LOCAL);
        // if (!s.contains(s2)) s = s2 + ROOT_PATH + s;
        return s;
    }

    private Path _fromLocal(File f) {
        // fail return new Path(ROOT_PATH + f.getName());
        return new Path(f.getAbsolutePath().replace(DEBUG_LOCAL, ROOT_PATH.toString()));
        //return new Path(f.getPath().replace(DEBUG_LOCAL, _myUri.toString()));
    }
}
