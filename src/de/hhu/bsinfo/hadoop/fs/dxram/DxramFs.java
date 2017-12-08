package de.hhu.bsinfo.hadoop.fs.dxram;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

public class DxramFs extends AbstractFileSystem {
    private DxramFileSystem dxramFileSystem;

    public DxramFs(URI uri, Configuration conf) throws URISyntaxException, IOException {
        this(new DxramFileSystem(), uri, conf);
    }

    public DxramFs(DxramFileSystem fs, URI uri, Configuration conf)
        throws URISyntaxException, IOException {
        super(uri, fs.getScheme(), true, 0);
        this.dxramFileSystem = fs;
        dxramFileSystem.initialize(uri, conf);
    }

    @Override
    public int getUriDefaultPort() {
        return 0;
    }

    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
        return DxramConfigKeys.getServerDefaults();
    }

    @Override
    public FsServerDefaults getServerDefaults(final Path f) throws IOException {
        //InodeTree.ResolveResult<AbstractFileSystem> res;
        //try {
        //    res = fsState.resolve(getUriPath(f), true);
        //} catch (FileNotFoundException fnfe) {
            return DxramConfigKeys.getServerDefaults();
        //}
        //return res.targetFileSystem.getServerDefaults(res.remainingPath);
    }

    @Override
    public FSDataOutputStream createInternal(
        Path f, EnumSet<CreateFlag> flag,
        FsPermission absolutePermission, int bufferSize, short replication, long blockSize,
        Progressable progress, Options.ChecksumOpt checksumOpt, boolean createParent
    ) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
        return dxramFileSystem.create(
            f, absolutePermission, true, bufferSize, replication,
            blockSize, progress
        );
    }

    @Override
    public void mkdir(Path dir, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
        dxramFileSystem.mkdirs(dir, permission);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return dxramFileSystem.delete(f, recursive);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return dxramFileSystem.open(f, bufferSize);
    }

    @Override
    public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return dxramFileSystem.setReplication(f, replication);
    }

    @Override
    public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
        dxramFileSystem.rename(src, dst);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return dxramFileSystem.getFileStatus(f);
    }

    @Override
    public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
        return dxramFileSystem.getStatus();
    }

    @Override
    public FileStatus[] listStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return dxramFileSystem.listStatus(f);
    }

    // -----------------------------------------------------------

    @Override
    public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return new BlockLocation[0];
    }

    @Override
    public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

    }

    @Override
    public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

    }

    @Override
    public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return null;
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {

    }
}
