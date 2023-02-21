package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.fs.common.ThreadSafeSimpleDateFormat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class Constants {
    private Constants(){}

    public static final String USER_AGENT = "us3-bigdata";

    public static final String VERSION = "1.4.0";

    /** 日期解析模板，解析如一下格式:
     *  "Sat, 07 Dec 2019 07:20:59 GMT"
     *  local 选择Locale.US还是default需要在多个地域测试验证
     */
    public static ThreadSafeSimpleDateFormat GMTDateTemplate = new ThreadSafeSimpleDateFormat();

    /** 上传的默认媒体类型 */
    public static String UPLOAD_DEFAULT_MIME_TYPE = "application/octet-stream";

    /** 文件目录的媒体类型1，也是老插件使用的风格 */
    public static String DIRECTORY_MIME_TYPE_1 = "file/path";

    /** 文件目录的媒体类型2，是新插件使用的风格 */
    public static String DIRECTORY_MIME_TYPE_2 = "application/x-directory";

    /** 目录的MIME类型 */
    public static Set<String> DIRECTORY_MIME_TYPES = new HashSet<>(
            Arrays.asList(Constants.DIRECTORY_MIME_TYPE_1, Constants.DIRECTORY_MIME_TYPE_2)
    );

    public static int MAX_FILE_SIZE_USE_PUT = 2<<21; // 4MB

    /** CheckSum 采用CRC32C的开关, 如果执行时显示设置-Dfs.ufile.checksum.type=CRC32C, 则在写入文件时，会计算CRC32C */
    public static String CHECKSUM_USE_CRC32C = "fs.ufile.checksum.type";
    public static String US3_CHECKSUM_USE_CRC32C = "fs.us3.checksum.type";

    /** 参数CHECKSUM_USE_CRC32C的值 */
    public static String CHECKSUM_USE_CRC32C_VALUE = "CRC32C";

    /** CheckSum算法名 */
    public static String CHECKSUM_ALGORITHM_NAME = "COMPOSITE-CRC32C";

    /** 拉取列表的默认限制大小 */
    public static int LIST_OBJECTS_DEFAULT_LIMIT = 1000;

    /** 列表API的基础重试等待时间，单位毫秒 */
    public static int LIST_TRY_DELAY_BASE_TIME = 50;

    /** 拉取列表的默认分隔符 */
    public static String LIST_OBJECTS_DEFAULT_DELIMITER = "/";

    /** 网络错误最大重试次数 */
    public static int NETWORK_DEFAULT_MAX_TRYTIMES = 11;

    /** PUT上传的默认最大重试次数 */
    public static int PUT_DEFAULT_MAX_TRYTIMES = 3;

    /** 分片上传的相关API默认最大重试次数 */
    public static int PART_DEFAULT_MAX_TRYTIMES = 3;

    /** 重试API的基础时间，单位毫秒 */
    public static int TRY_DELAY_BASE_TIME = 500;

    /** GET下载的默认最大重试次数 */
    public static int GET_DEFAULT_MAX_TRYTIMES = 11;

    /** 所有API最大重试次数 */
    public static int DEFAULT_MAX_TRYTIMES = 11;

    /** LIST API最大重试次数 */
    public static int LIST_MAX_TRYTIMES = 5;

    /** API 返回404结果 */
    public static int API_NOT_FOUND_HTTP_STATUS = 404;


    /** 所有API最大重试次数 */
    public static int DEFAULT_TIMEOUT = 30;

    /**
     *  涉及HDFS文件相关元数据信息的指标
     */
    /** 文件元数据之拥有者索引关键字 */
    public static String HDFS_OWNER_KEY = "hdfs-owner";

    /** 文件元数据之所属组索引关键字 */
    public static String HDFS_GROUP_KEY = "hdfs-group";

    /** 文件元数据之校验和索引关键字 */
    public static String HDFS_CHECKSUM_KEY = "hdfs-crc32c";

    /** 文件元数据之拥有者权限位索引关键字 */
    public static String HDFS_PERMISSION_USER_KEY = "hdfs-user-ac";

    /** 文件元数据之用户组权限位索引关键字 */
    public static String HDFS_PERMISSION_GROUP_KEY = "hdfs-group-ac";

    /** 文件元数据之其他用户者权限位索引关键字 */
    public static String HDFS_PERMISSION_OTHER_KEY = "hdfs-other-ac";

    /** 文件元数据之Sticky权限位索引关键字 */
    public static String HDFS_PERMISSION_STICKY_KEY = "hdfs-sticky";

    /** 文件元数据之备份数关键字 */
    public static String HDFS_REPLICATION_NUM_KEY = "hdfs-rep";

    /** 文件元数据之块大小关键字 */
    public static String HDFS_BLOCK_SIZE_KEY = "hdfs-bls";

    /**
     * 文件元数据之块默认大小
     * 该参数大小会影响MAP TASK数量
     */
    public static Long DEFAULT_HDFS_BLOCK_SIZE = (long)288*1024*1024;

    /** 文件只可写之元数据内容 */
    public static String HDFS_FILE_ONLY_WRITE = "write";

    /** 文件可写可执行之元数据内容 */
    public static String HDFS_FILE_WRITE_EXECUTE = "write-execute";

    /** 文件只可读之元数据内容 */
    public static String HDFS_FILE_ONLY_READ = "read";

    /** 文件可读可执行之元数据内容 */
    public static String HDFS_FILE_READ_EXECUTE = "read-execute";

    /** 文件可读可写之元数据内容 */
    public static String HDFS_FILE_READ_WRITE = "read-write";

    /** 文件可读可写可执行之元数据内容 */
    public static String HDFS_FILE_ALL = "all";

    /** 文件无权限之元数据内容 */
    public static String HDFS_FILE_NONE = "none";

    /** 文件可执行之元数据内容 */
    public static String HDFS_FILE_EXECUTE = "execute";

    /**
     * 涉及存储类型的相关信息
     */
    /** 以下两个字段表示，每UN_FREEZING_BLOCK_TIME_PER_BYTE字节数睡眠UN_FREEZING_BLOCK_TIME毫秒*/
    public static long UN_FREEZING_BLOCK_TIME = 100;

    /** 冷存文件每多少字节睡眠的毫秒数 */
    public static long UN_FREEZING_BLOCK_TIME_PER_BYTE = 4*1024*1024;

    /** 冷存小文件激活后等待访问时间, 单位毫秒 */
    public static long SMALL_FILE_SIZE_UN_FREEZING_BLOCK_TIME = 30;

    /** 标准存储类型 */
    //public static String UFILE_STORAGE_STANDARD = "STANDARD";

    /** 低频存储类型 */
    //public static String UFILE_STORAGE_IA = "IA";

    /** 冷存存储类型 */
    public static String UFILE_STORAGE_ARCHIVE = "ARCHIVE";

    /** 过期缓冲时间，单位毫秒，10分钟 */
    public static long UFILE_STORAGE_ARCHIVE_BUFFEER_TIME = 10 * 60;

    /**
     * 涉及缓存相关的信息
     */
    public static UFileMetaStore ufileMetaStore = new UFileMetaStore();

    /** 空buf */
    public static byte[] empytBuf = new byte[0];

    /** 是否使用自定义hashCode函数 */
    public static boolean useSelfDefHashCode = true;

    public static String superGroup = "supergroup";

    public static int IO_FILE_BUFFER_SIZE_DEFAULT = 65536;

    /**
     * 用于直接从core-site.xml读取相关信息的Key
     */
    // ufile public API/token key
    public static final String CS_ACCESS_KEY = "fs.ufile.access.key";
    public static final String CS_US3_ACCESS_KEY = "fs.us3.access.key";

    // ufile private API/token key
    public static final String CS_SECRET_KEY = "fs.ufile.secret.key";
    public static final String CS_US3_SECRET_KEY = "fs.us3.secret.key";

    // ufile domain
    public static final String CS_ENDPOINT = "fs.ufile.endpoint";
    public static final String CS_US3_ENDPOINT = "fs.us3.endpoint";

    // ufile bigdata plugin log level， default is info
    // value can be 'error', 'info', 'debug', 'data'
    public static final String CS_LOG_LEVEL_KEY = "fs.ufile.log.level";
    public static final String CS_US3_LOG_LEVEL_KEY = "fs.us3.log.level";
    public static final String DEFAULT_CS_LOG_LEVEL_KEY = "info";

    // unit is Byte, default is 64*1024
    public static final String CS_SOCKET_RECV_BUFFER = "fs.ufile.socket.recv.buffer";
    public static final String CS_US3_SOCKET_RECV_BUFFER = "fs.us3.socket.recv.buffer";
    public static final int DEFAULT_CS_SOCKET_RECV_BUFFER = 2 << 15; // 64KB

    public static final String CS_THREAD_POOL_SIZE = "fs.ufile.thread.pool.size";
    public static final String CS_US3_THREAD_POOL_SIZE = "fs.us3.thread.pool.size";
    public static final int DEFAULT_CS_THREAD_POOL_SIZE = 8;

    /**
     * Bucket在接入大数据场景之前是否创建了UFile所谓的‘目录’，默认认为创建过
     * 如果考虑到性能，可以设置fs.us3.bucket.bigdata.only为true.
     * 这是因为如果使用该场景前，有UFile的目录，但考虑到兼容性，需要对UFile实际未创建的目录文件做创建;
     */
    public static final String CS_US3_IS_BIGDATA_ONLY = "fs.us3.bucket.bigdata.only";

    public static final String CS_UFILE_USE_MDS = "fs.ufile.metadata.use";
    public static final String CS_US3_USE_MDS = "fs.us3.metadata.use";

    public static final String CS_UFILE_MDS_HOST = "fs.ufile.metadata.host";
    public static final String CS_US3_MDS_HOST = "fs.us3.metadata.host";

    //自定义的zk地址
    public static final String CS_UFILE_MDS_ZOOKEEPER_ADDRS = "fs.ufile.metadata.zookeeper.addrs";
    public static final String CS_US3_MDS_ZOOKEEPER_ADDRS = "fs.us3.metadata.zookeeper.addrs";

    public static final String CS_UFILE_USE_ASYNC_WIO = "fs.ufile.async.wio.use";
    public static final String CS_US3_USE_ASYNC_WIO = "fs.us3.async.wio.use";

    public static final String CS_UFILE_ASYNC_WIO_PARALLEL = "fs.ufile.async.wio.parallel";
    public static final String CS_US3_ASYNC_WIO_PARALLEL = "fs.us3.async.wio.parallel";
    public static final int DEFAULT_CS_ASYNC_WIO_PARALLEL = 2;

    public static final String CONTENT_LENGTH_FOR_NOTIFY = "content-length";
    public static final String CS_US3_GENERATE_MD5 = "fs.us3.generate.md5";
    /** 文件元数据之MD5 */
    public static final String META_MD5_HASH = "md5-hash";

    // hadoop使用的zk地址
    public static final String HD_ZOOKEEPER_ADDRS = "ha.zookeeper.quorum";

    public static final String CS_UFILE_RETRY_TIMES = "fs.ufile.retryTimes";

    public static final String CS_US3_RETRY_TIMES = "fs.us3.retryTimes";

    public static final String CS_UFILE_TIMEOUT = "fs.ufile.timeout";

    public static final String CS_US3_TIMEOUT = "fs.us3.timeout";
}


