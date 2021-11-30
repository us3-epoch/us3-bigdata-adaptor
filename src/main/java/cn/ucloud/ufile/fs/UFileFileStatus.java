package cn.ucloud.ufile.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.Map;

public class UFileFileStatus extends FileStatus {
    public static UFileFileStatus[] ufs = new UFileFileStatus[0];

    private UFileFileSystem fs;

    /** 文件的CRC32C校验和 */
    private String hexCrc32c;

    /** 用于覆盖文件所属者、用户组 */
    private String overrideUserName;

    private String overrideGroupName;

    /** 存储类型 */
    private String storageType;

    /** 冷存文件的激活状态，非冷存文件为null */
    private ObjectRestoreExpiration ore;

    /**
     * 在缓存中的过期时间
     * 如果>0,则认为404缓,且为超时时间
     */
    private int fzfCacheTimeout = 0;

    private int cacheTimeout = (int)(System.currentTimeMillis())/1000 + 10;

    private String base64Md5;

    public UFileFileStatus(Path p, UFileFileStatus ufs) {
        super(ufs.getLen(),
            ufs.isDirectory(),
            ufs.getReplication(),
            ufs.getBlockSize(),
            ufs.getModificationTime(),
            ufs.getAccessTime(),
            ufs.getPermission(),
            ufs.getOwner(),
            ufs.getGroup(),
            p);
        this.hexCrc32c = ufs.getHexCrc32c();
        this.overrideUserName = ufs.getOverrideUserName();
        this.overrideGroupName = ufs.getOverrideGroupName();
        this.storageType = ufs.getStorageType();
        this.ore = ufs.getORE();
        this.base64Md5 = ufs.getBase64Md5();
    }

    public UFileFileStatus(long length, boolean isDir, int blockReplication, long blockSize, long modTime, Path path) {
        super(length, isDir, blockReplication, blockSize, modTime, path);
    }

    public UFileFileStatus(long length, boolean isdir,
                           int block_replication,
                           long blocksize, long modification_time, long access_time,
                           FsPermission permission, String owner, String group,
                           Path path) {
        super(length, isdir, block_replication,
                blocksize, modification_time, access_time,
                permission, owner, group, path);
    }

    public static FsPermission parsePermission(Map<String, String> userMeta) {
        if (userMeta == null) return null;
        String userAc = userMeta.get(Constants.HDFS_PERMISSION_USER_KEY);
        String groupAc = userMeta.get(Constants.HDFS_PERMISSION_GROUP_KEY);
        String otherAc = userMeta.get(Constants.HDFS_PERMISSION_OTHER_KEY);
        boolean sticky = UFileUtils.DecodeFsSticky(userMeta.get(Constants.HDFS_PERMISSION_STICKY_KEY));

        if (userAc != null && groupAc != null && otherAc != null) {
            FsAction userFsAc = UFileUtils.DecodeFsAction(userAc);
            FsAction groupFsAc = UFileUtils.DecodeFsAction(groupAc);
            FsAction otherFsAc = UFileUtils.DecodeFsAction(otherAc);
            if (userFsAc != null && groupFsAc != null && otherFsAc != null) {
                return new FsPermission(userFsAc, groupFsAc, otherFsAc, sticky);
            }
        }
        return null;
    }

    private static String parserMetaData(Map<String, String> userMeta, String key) {
        if (userMeta == null) {
            return null;
        }
        return userMeta.get(key);
    }

    private static Long parserMetaDataLong(Map<String, String> userMeta, String key) {
        if (userMeta == null) {
            return null;
        }
        String value = userMeta.get(key);
        if (value != null && !value.equals("")) {
            return Long.parseLong(value);
        } else {
            return (long)0;
        }
    }

    private static int parserMetaDataInt(Map<String, String> userMeta, String key) {
        if (userMeta == null) {
            return 0;
        }
        String value = userMeta.get(key);
        if (value != null && !value.equals("")) {
            return Integer.parseInt(value);
        } else {
            return 0;
        }
    }

    private static String parserOwnerKey(UFileFileSystem fs, Map<String, String> userMeta) {
        String owner = parserMetaData(userMeta, Constants.HDFS_OWNER_KEY);
        if (owner == null) {
            return fs.getUsername();
        }

        return owner;
    }

    private static String parserGroupKey(UFileFileSystem fs, Map<String, String> userMeta) {
        String group = parserMetaData(userMeta, Constants.HDFS_GROUP_KEY);
        if (group == null) {
            return Constants.superGroup;
        }

        return group;
    }

    private static String parserHexCrc32c(Map<String, String> userMeta) {
        return parserMetaData(userMeta, Constants.HDFS_CHECKSUM_KEY);
    }

    private static int parserBlockReplication(Map<String, String> userMeta) {
        int rep = parserMetaDataInt(userMeta, Constants.HDFS_REPLICATION_NUM_KEY);
        if (rep == 0) return 3;
        return rep;
    }

    private static long parserBlockSize(Map<String, String> userMeta) {
        Long bs = parserMetaDataLong(userMeta, Constants.HDFS_BLOCK_SIZE_KEY);
        if (bs == null || bs == 0) return Constants.DEFAULT_HDFS_BLOCK_SIZE;
        return bs;
    }

    private static String parserBase64MD5(Map<String, String> userMeta) {
        return parserMetaData(userMeta, Constants.META_MD5_HASH);
    }
    /**
     * 该构造函数用户获取文件信息构造文件信息时，利用携带文件相关元数据恢复文件属性
     * @param length
     * @param isdir
     * @param modification_time
     * @param access_time
     * @param path
     * @param metaData
     */
    public UFileFileStatus(UFileFileSystem fs,
                           long length,
                           boolean isdir,
                           long modification_time,
                           long access_time,
                           Path path,
                           Map<String, String> metaData
                           ) {
        super(length, isdir, UFileFileStatus.parserBlockReplication(metaData), UFileFileStatus.parserBlockSize(metaData), modification_time, access_time, UFileFileStatus.parsePermission(metaData),
                UFileFileStatus.parserOwnerKey(fs, metaData), UFileFileStatus.parserGroupKey(fs, metaData), path);
        this.hexCrc32c = UFileFileStatus.parserHexCrc32c(metaData);
        this.base64Md5 = UFileFileStatus.parserBase64MD5(metaData);
        this.fs = fs;
    }

    public String getHexCrc32c() { return hexCrc32c; }

    public void setOverrideUserName(String name) { overrideUserName = name; }

    public void setOverrideGroupName(String name) { overrideGroupName = name; }

    public String getOverrideUserName() { return overrideUserName; }

    public String getOverrideGroupName() { return overrideGroupName; }

    public void setORE(ObjectRestoreExpiration ore) { this.ore = ore; }

    public ObjectRestoreExpiration getORE() { return this.ore; }

    public void setStorageType(String type) { storageType = type; }

    public String getStorageType() { return storageType; }

    public String getBase64Md5() { return base64Md5; }

    /**
     * 该函数用于根据冷存文件大小，以及总共累计等待时间来进行睡眠阻塞
     * @param times 执行了多少次
     * @return 目前总共睡眠的时间，叠加了本次的睡眠时间，单位ms
     */
    public void blockTimeForUnFreezing(int times) {
        long sleepTime = Constants.SMALL_FILE_SIZE_UN_FREEZING_BLOCK_TIME;
        if (getLen() > Constants.UN_FREEZING_BLOCK_TIME_PER_BYTE) {
            sleepTime = (getLen()/Constants.UN_FREEZING_BLOCK_TIME_PER_BYTE)*
                    Constants.UN_FREEZING_BLOCK_TIME;
        }

        for (int i = 0; i < times ; i++ ) {
            sleepTime = sleepTime/(2);
            if (sleepTime == 0) {
                sleepTime = 10;
                break;
            }
        }

        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 是否是低频
     * @return
     */
    public boolean isArchive() {
        return storageType.equals(Constants.UFILE_STORAGE_ARCHIVE);
    }

    public static enum RestoreStatus {
        /** 未激活 */
        UNRESTORE("unrestore"),
        /** 正在解冻中 */
        UNFREEZING("unfreezing"),
        /** 已经解冻成功 */
        RESTOED("restored"),
        /** 未知状态 */
        UNKNOWN("unknown");

        final private String date;

        RestoreStatus(String s) { date = s; }
    };

    public RestoreStatus restoreStatus() {
        if (ore != null) {
            if (ore.onGoing) {
                return RestoreStatus.UNFREEZING;
            } else if (System.currentTimeMillis()/1000 +
                    Constants.UFILE_STORAGE_ARCHIVE_BUFFEER_TIME < ore.expiration){
                return RestoreStatus.RESTOED;
            }
        };
        return RestoreStatus.UNRESTORE;
    }

    public void setCacheTimeout(int timeout) { fzfCacheTimeout = timeout; }

    public int getCacheTimeout() { return fzfCacheTimeout; }

    public static UFileFileStatus Cache404() {
        UFileFileStatus fs = new UFileFileStatus(0, false, 0, 0, 0, null);
        fs.setCacheTimeout((int)(System.currentTimeMillis()/1000 + 3));
        return fs;
    }

    public boolean is404Cache() {
        return fzfCacheTimeout>0?true:false;
    }

    public boolean timeOut404Cache() {
        if (fzfCacheTimeout >= (System.currentTimeMillis()/1000)) {
            return false;
        } else {
            return true;
        }
    }

    public boolean timeOutCache() {
        if (cacheTimeout >= (System.currentTimeMillis()/1000)) {
            return false;
        } else {
            return true;
        }
    }
}
