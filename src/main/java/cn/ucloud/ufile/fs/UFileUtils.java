package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.exception.UfileServerException;
import com.sun.istack.NotNull;
import com.sun.istack.Nullable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.log4j.BasicConfigurator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


enum LOGLEVEL { TRACE, DEBUG, INFO, ERROR }

class OSMeta {
    private String bucket;
    private String key;

    public OSMeta(String bt, String ky){
        this.bucket = bt;
        this.key = ky;
    }

    public String getBucket() { return bucket; }

    public String getKey() { return key; }

    public void setKey(String key) { this.key = key; }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        if (this.getClass() != obj.getClass()){
            return false;
        }

        OSMeta osMeta = (OSMeta)obj;
        return bucket.equals(osMeta.getBucket()) && key.equals(osMeta.getKey());
    }
    @Override
    public String toString() {
        return "bucket:" + bucket + " key:" + key;
    }
};

class ListUFileResult {
    public List<UFileFileStatus> fss;
    public String nextMarker;
}

class ObjectRestoreExpiration {
    public ObjectRestoreExpiration(){}

    public ObjectRestoreExpiration(boolean onGoing, long expiration){
        this.onGoing = onGoing;
        this.expiration = expiration;
    }

    /** 是否正在解冻 */
    public boolean onGoing;
    /** 解冻成功后的过期时间，单位ms */
    public long expiration;
}

public class UFileUtils {
    //public static final Logger LOG = LoggerFactory.getLogger(UFileFileSystem.class);

    /**
     * 转换日志级别为指定日志级别枚举
     * @param level
     * @return
     */
    public static LOGLEVEL ParserLogLevel(String level) {
        if (level == null) {
            return LOGLEVEL.INFO;
        }

        switch (level.toLowerCase()) {
            case "trace":
                return LOGLEVEL.TRACE;
            case "debug":
                return LOGLEVEL.DEBUG;
            case "info":
            case "":
                return LOGLEVEL.INFO;
            case "error":
                return LOGLEVEL.ERROR;
        }
        return LOGLEVEL.DEBUG;
    }

    public static OSMeta ParserPath(URI uri, Path workDir, Path path) {
        path = path.makeQualified(uri, workDir);

        if (!path.isAbsolute()) {
            path = new Path(workDir, path);
        }

        uri = path.toUri();
        if (uri.getScheme() != null && uri.getPath().isEmpty()) {
            return new OSMeta(uri.getAuthority(), "");
        }

        return new OSMeta(uri.getAuthority(), uri.getPath().substring(1));
    }

    public static IOException TranslateException(@Nullable String operation, String path, Exception e) {
        String message = String.format("%s%s: %s\n",
                operation,
                " on " + path,
                e);
        if (!(e instanceof UfileServerException)) {
            return new IOException(message);
        }

        IOException ioe = new IOException(message);
        UfileServerException use = (UfileServerException)e;
        switch (use.getErrorBean().getResponseCode()) {
        case 404:
            ioe = new FileNotFoundException(message);
            ioe.initCause(use);
            break;
        }

        return ioe;
    }

    /**
     * 判断对象是否代表一个目录?.
     * 必须是该文件名不为空，且最后一个字符是'/'，同时文件大小为0
     * @param name 对象名字
     * @param size 对象大小
     * @return 如果满足是一个目录的条件返回true
     */
    public static boolean IsDirectory(final String name, final long size) {
        return !name.isEmpty()
                && name.charAt(name.length() - 1) == '/'
                && size == 0L;
    }

    private static void Log(LOGLEVEL expLvl, LOGLEVEL limitLvl, @NotNull String format, Object ... args) {
        if (expLvl.ordinal() >= limitLvl.ordinal()) {
            String data = String.format(format,args);
            String date = Constants.GMTDateTemplate.format(new Date(System.currentTimeMillis()));
            System.out.printf("[%s] [%s] %s\n", date, expLvl, data);
        }
    }

    public static void Debug(LOGLEVEL limitLvl, @NotNull String format, Object ... args) {
        Log(LOGLEVEL.DEBUG, limitLvl, format, args);
    }

    public static void Info(LOGLEVEL limitLvl, @NotNull String format, Object ... args) {
        Log(LOGLEVEL.INFO, limitLvl, format, args);
    }

    public static void Error(LOGLEVEL limitLvl, @NotNull String format, Object ... args) {
        Log(LOGLEVEL.ERROR, limitLvl, format, args);
    }

    public static void Trace(LOGLEVEL limitLvl, @NotNull String format, Object ... args) {
        Log(LOGLEVEL.TRACE, limitLvl, format, args);
    }

    public static String EncodeFsAction(FsAction ac) {
        if (0 == FsAction.READ_EXECUTE.compareTo(ac)) return Constants.HDFS_FILE_READ_EXECUTE;
        if (0 == FsAction.READ_WRITE.compareTo(ac)) return Constants.HDFS_FILE_READ_WRITE;
        if (0 == FsAction.WRITE.compareTo(ac)) return Constants.HDFS_FILE_ONLY_WRITE;
        if (0 == FsAction.WRITE_EXECUTE.compareTo(ac)) return Constants.HDFS_FILE_WRITE_EXECUTE;
        if (0 == FsAction.READ.compareTo(ac)) return Constants.HDFS_FILE_ONLY_READ;
        if (0 == FsAction.EXECUTE.compareTo(ac)) return Constants.HDFS_FILE_EXECUTE;
        if (0 == FsAction.ALL.compareTo(ac)) return Constants.HDFS_FILE_ALL;
        if (0 == FsAction.NONE.compareTo(ac)) return Constants.HDFS_FILE_NONE;
        return "";
    }

    public static FsAction DecodeFsAction(String ac) {
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_READ_EXECUTE)) return FsAction.READ_EXECUTE;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_READ_WRITE)) return FsAction.READ_WRITE;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_ONLY_WRITE)) return FsAction.WRITE;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_WRITE_EXECUTE)) return FsAction.WRITE_EXECUTE;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_ONLY_READ)) return FsAction.READ;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_EXECUTE)) return FsAction.EXECUTE;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_ALL)) return FsAction.ALL;
        if (ac.toLowerCase().equals(Constants.HDFS_FILE_NONE)) return FsAction.NONE;
        return null;
    }

    public static String EncodeFsSticky(boolean sticky) {
        if (sticky) return "true";
        else return "false";
    }

    public static boolean DecodeFsSticky(String sticky) {
        if (sticky == null) return false;
        switch (sticky.toLowerCase()) {
            case "true":
                return true;
            case "false":
                return false;
        }
        return false;
    }

    public static String EncodeBlockSize(long bsize) {
        return Long.toString(bsize);
    }

    public static long DecodeBlockSize(String bsize) {
        return Long.parseLong(bsize);
    }

    public static String EncodeReplication(short rep) {
        return Short.toString(rep);
    }

    public static long DecodeReplication(String rep) {
        return Short.parseShort(rep);
    }

    private static Pattern restorePattern = Pattern.compile("\"(.*?)\"");

    public static ObjectRestoreExpiration ParserRestore(String headerVal) throws ParseException {
        if (headerVal.equals("")) return null;
        headerVal = headerVal.trim();
        Matcher m = restorePattern.matcher(headerVal);
        if (m == null) return null;
        int i = 0;
        ObjectRestoreExpiration ore = new ObjectRestoreExpiration();
        for (; i < 2 ; i++) {
            if (m.find()) {
                String val = m.group();
                if (val.length()<= 2) return null;
                val = val.substring(1,val.length()-1);
                switch (i) {
                    case 0:
                        /** 复用该解析函数 */
                        ore.onGoing = DecodeFsSticky(val);
                        /** 当正在解冻过程中，不需要关注解冻过期时间 */
                        if (ore.onGoing) return ore;
                        break;
                    case 1:
                        System.out.println(val);
                        ore.expiration = Constants.GMTDateTemplate.parse(val).getTime()/1000;
                        break;
                }
            }
        }
        return ore;
    }

    public static boolean isArchive(String type) { return type.equals(Constants.UFILE_STORAGE_ARCHIVE); }

    /**
     * @param fs
     * @param prefix
     * @param maxTryTimes
     * @param exist true 表示确保文件存在，false 表示确保不存在
     * @throws IOException
     */
    public static void KeepListFileExistConsistency(UFileFileSystem fs, String prefix, int maxTryTimes, boolean exist) throws IOException {
        if (prefix.endsWith("/")) { prefix = prefix.substring(0, prefix.length()-1);}
        if (fs.getCfg().isUseMDS()) { return; }
        int tryTimes = 0;
        long begin = System.currentTimeMillis();
        boolean needCheck = false;
        while (true) {
            // 为了在单独使用适配器时保证列表服务一致性
            UFileFileStatus[] ufss = fs.genericInnerListStatusWithSize(prefix,
                    true, true, 1, true);
            String key = "";
            if (ufss.length == 0) {
                needCheck = true;
            } else {
                for (UFileFileStatus ufs: ufss) {
                    OSMeta osMeta = UFileUtils.ParserPath(fs.getUri(), fs.getWorkingDirectory(), ufs.getPath());
                    if (osMeta.getKey().equals(prefix)) {
                        needCheck = false;
                        break;
                    } else { 
                        key = osMeta.getKey();
                        needCheck = true; 
                    }
                }
            }

            if (!exist) {
                needCheck = !needCheck;
            }

            if (needCheck) {
                if (tryTimes > maxTryTimes) {
                    long costtime = System.currentTimeMillis() - begin;
                    throw new IOException("It took " + Long.valueOf(costtime) + " milliseconds, or due to the consistency of the us3 list service, the '" +
                               prefix + "' file just uploaded could not be detected, and osMeta.getKey() is "+ key);
                }
                tryTimes++;
                try {
                    Thread.sleep(tryTimes*Constants.LIST_TRY_DELAY_BASE_TIME);
                } catch (InterruptedException e) { /* do nothing */}
            } else { // 证明列表服务已经同步到
                return;
            }
        }
    }

    /**
     * 确保目录下为空
     * @param fs
     * @param prefix
     * @param maxTryTimes
     * @throws IOException
     */
    public static void KeepListDirExpectConsistency(UFileFileSystem fs, String prefix, int maxTryTimes, int expectCount) throws IOException {
        if (fs.getCfg().isUseMDS()) { return; }
        if (!prefix.endsWith("/")) { prefix += "/"; }

        int tryTimes = 0;
        long begin = System.currentTimeMillis();
        boolean needCheck = false;
        while (true) {
            // 为了在单独使用适配器时保证列表服务一致性
            UFileFileStatus[] ufss = fs.genericInnerListStatusWithSize(prefix,
                    true, false, Constants.LIST_OBJECTS_DEFAULT_LIMIT, false);
            if (ufss.length == expectCount) {
                needCheck = false;
            } else {
                needCheck = true;
            }

            if (needCheck) {
                if (tryTimes > maxTryTimes) {
                    long costtime = System.currentTimeMillis() - begin;
                    throw new IOException("It took " + Long.valueOf(costtime) + " milliseconds, or due to the consistency of the us3 list service, the '" +
                            prefix + "' dir' child expect count is " + Integer.valueOf(expectCount) + " , but is " + Integer.valueOf(ufss.length));
                }
                tryTimes++;
                try {
                    Thread.sleep(tryTimes*Constants.LIST_TRY_DELAY_BASE_TIME);
                } catch (InterruptedException e) { /* do nothing */}
            } else { // 证明列表服务已经同步到
                return;
            }
        }
    }
}
