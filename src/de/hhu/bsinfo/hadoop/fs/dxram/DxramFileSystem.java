package de.hhu.bsinfo.hadoop.fs.dxram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DxramFileSystem extends FileSystem {

    private static final Path ROOT_PATH = new Path(Path.SEPARATOR);
    private static final String SCHEME = "dxram";

    private URI _myUri;
    private Path _workingDir;

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
            _workingDir = this.getHomeDirectory();
        } catch (URISyntaxException e) {
            throw new IOException("URISyntax exception: " + theUri);
        }
    }

        @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream create(
        Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress
    ) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        _workingDir = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
        return _workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return null;
    }
}
