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
    private static final String DEBUG_LOCAL = "/tmp/myfs/";

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

    private void doLog(String str) {
        System.out.println("DxramFileSystem: " + str);
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
            _workingDir = this.getHomeDirectory();
            System.out.printf("working Dir: %s\n", _workingDir.toString());

        } catch (URISyntaxException e) {
            throw new IOException("URISyntax exception: " + theUri);
        }
    }

        @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        File file = _toLocal(f);
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName() + " " + f.toString());

        if (getFileStatus(f).isDirectory()) throw new IOException("is directory");
        
        byte[] data = new byte[(int) file.length()];
        FileInputStream fis = new FileInputStream(file);
        fis.read(data);
        fis.close();
        DxramInputStream dxins = new DxramInputStream(data);
        FSDataInputStream dais = new FSDataInputStream(dxins);
        return dais;
    }

    @Override
    public FSDataOutputStream create(
        Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress
    ) throws FileAlreadyExistsException, IOException {
        File file = _toLocal(f);
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName() + " " + file.toString());
        if (file.exists() && getFileStatus(f).isDirectory()) {
            throw new IOException("existing directory");
        }
        if (file.exists()) throw new FileAlreadyExistsException("file still exists");
        file.createNewFile();
        OutputStream out = new FileOutputStream(file);
        FSDataOutputStream outs = new FSDataOutputStream(out, (Statistics)null);
        return outs;
    }

    @Override
    public FSDataOutputStream append(
        Path f, int bufferSize, Progressable progress
    ) throws IOException {
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        File file = _toLocal(src);
        File file2 = _toLocal(dst);

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
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        File file = _toLocal(f);
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
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        _workingDir = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        return _workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws FileAlreadyExistsException, IOException {
        String s = f.toString().replace(_myUri.toString(), DEBUG_LOCAL);
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName() + " " + s);
        File file = new File(s);
        if (file.exists()) throw new FileAlreadyExistsException("mkdirs: " + s + " exists");
        return file.mkdirs();
    }

    @Override
    public FileStatus[] listStatus(Path p) throws FileNotFoundException, IOException {

        /**
         * @todo hack: listStatus [] I get dxram://abook.localhost.fake:9000/tmp/myfs
         *     and not listStatus [] dxram://abook.localhost.fake:9000/
         */
        String check = p.toString() + "/";
        if (check.startsWith(_myUri.toString()) && check.endsWith(DEBUG_LOCAL)) {
            p = new Path(_myUri);
        }

        doLog(Thread.currentThread().getStackTrace()[1].getMethodName() + " [] " + p.toString());

        ArrayList<FileStatus> statusArrayList = new ArrayList<>();
        FileStatus fileStatus = getFileStatus(p);

        if (fileStatus.isFile()) {
            statusArrayList.add(fileStatus);

        } else if (fileStatus.isDirectory()) {
            File file = _toLocal(p);
            for (File childFile : file.listFiles()) {
                statusArrayList.add(_getFileStatus(childFile));
            }
        }

        return statusArrayList.toArray(new FileStatus[statusArrayList.size()]);
    }

    @Override
    public FileStatus getFileStatus(Path p) throws FileNotFoundException, IOException {
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName() + " " + p.toString());
        File file = _toLocal(p);
        return _getFileStatus(file);
        
        /*
        Path absF = fixRelativePart(p);
		return new FileSystemLinkResolver<FileStatus>() {
		  @Override
		  public FileStatus doCall(final Path p) throws IOException {
			File file = _toLocal(p);
			FileStatus fi = _getFileStatus(file);
			if (fi != null) {
			  return fi;
			} else {
			  throw new FileNotFoundException("File does not exist: " + p);
			}
		  }
		  @Override
		  public FileStatus next(final FileSystem fs, final Path p)
			  throws IOException {
			return fs.getFileStatus(p);
		  }
		}.resolve(this, absF);
		*/
    }

    public FileStatus _getFileStatus(File file) throws FileNotFoundException, IOException {
        Path p = _fromLocal(file);
        if (!file.exists()) {

            throw new FileNotFoundException("_getFileStatus: " + p.toString() + " not exists");
            //throw new IOException("_getFileStatus: " + p.toString() + " not exists");
        }
        long blocksize = getServerDefaults(p).getBlockSize();
        return new FileStatus(
            file.length(),
            file.isDirectory(),
            0, blocksize, 0L, 0L, (FsPermission)null, (String)null, (String)null,
            p
        );
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


    @Override
    protected Path fixRelativePart(Path p) {
        doLog(Thread.currentThread().getStackTrace()[1].getMethodName());
        return super.fixRelativePart(p);
    }



    private File _toLocal(Path p) {
        String s = p.toString().replace(_myUri.toString(), DEBUG_LOCAL);
        //System.out.print("toLocal: " + p.toString() + " -> "+ s + "\n");
        
        // String s2 = _workingDir.toString().replace(_myUri.toString(), DEBUG_LOCAL);
        // if (!s.contains(s2)) s = s2 + ROOT_PATH + s;
        return new java.io.File(s);
    }

    private Path _fromLocal(File f) {
        Path p = new Path(f.getAbsolutePath().replace(DEBUG_LOCAL, _myUri.toString()));
        //System.out.print("fromLocal: " + f.getAbsolutePath() + " -> "+ p + "\n");

        /// @todo hack, too!!
        if ((f.getAbsolutePath()+"/").equals(DEBUG_LOCAL)) {
            return new Path(_myUri);
        }
        return p;
    }
}
