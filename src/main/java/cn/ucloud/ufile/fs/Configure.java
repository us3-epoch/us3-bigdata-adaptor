package cn.ucloud.ufile.fs;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Configure {
    // 配置值
    /**
     * 访问公钥
     */
    private String PublicKey;

    /**
     * 访问私钥
     */
    private String PrivateKey;

    /**
     * 访问UFile的域名后缀
     */
    private String EndPoint;

    /**
     * 读取UFile文件流的缓冲大小
     */
    private int ReadBufferSize;

    /**
     * 写UFile文件流采用PUT上传时的文件大小
     */
    private int MaxFileSizeUsePut;

    /**
     * 日志级别
     */
    private LOGLEVEL LogLevel;

    /**
     *  当写入文件的时候，是否开启CRC32C校验
     */
    private boolean CRC32COpen = false;

    /**
     *  Hadoop集群是否仅需要访问从Hadoop集群写入US3存储空间(桶或Bucket)的数据，而不考虑从SDK或者其他API工具上传到US3存储空间的数据
     */
    private boolean IsBigDataOnly;

    /**
     * 是否使用元数据服务
     */
    private boolean UseMDS = false;
    private String MDSHost;

    private String hdZookeeperAddresses;
    private String customZookeeperAddresses;

    public int getIoTimeout() {
        return ioTimeout;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    private int ioTimeout;

    private int retryTimes;
    /**
     *
     * 采用异步IO写入, 并发数
     */
    private boolean UseAsyncWIO = false;
    private int asyncWIOParallel;

    private boolean generateMD5 = false;

    public void Parse(Configuration cfg) throws IOException {
        PublicKey = cfg.get(Constants.CS_US3_ACCESS_KEY, "");
        if (PublicKey.isEmpty()) { PublicKey = cfg.get(Constants.CS_ACCESS_KEY); }

        PrivateKey = cfg.get(Constants.CS_US3_SECRET_KEY, "");
        if (PrivateKey.isEmpty()) { PrivateKey = cfg.get(Constants.CS_SECRET_KEY); }

        EndPoint =  cfg.get(Constants.CS_US3_ENDPOINT, "");
        if (EndPoint.isEmpty()) { EndPoint = cfg.get(Constants.CS_ENDPOINT); }

        /** 保证上传采用PUT的最大阈值目前写死4MB*/
        MaxFileSizeUsePut = Constants.MAX_FILE_SIZE_USE_PUT;

        ReadBufferSize = cfg.getInt(Constants.CS_US3_SOCKET_RECV_BUFFER, 0);
        if (ReadBufferSize == 0) { ReadBufferSize = cfg.getInt(Constants.CS_SOCKET_RECV_BUFFER, Constants.DEFAULT_CS_SOCKET_RECV_BUFFER ); }

        String logLevelStr = cfg.get(Constants.CS_US3_LOG_LEVEL_KEY, "");
        if (logLevelStr.isEmpty()) { logLevelStr = cfg.get(Constants.CS_LOG_LEVEL_KEY, Constants.DEFAULT_CS_LOG_LEVEL_KEY); }
        LogLevel = UFileUtils.ParserLogLevel(logLevelStr);

        String crc32cValue = cfg.get(Constants.US3_CHECKSUM_USE_CRC32C);
        if (crc32cValue == null) { crc32cValue = cfg.get(Constants.CHECKSUM_USE_CRC32C); }
        if (crc32cValue != null && crc32cValue.equals(Constants.CHECKSUM_USE_CRC32C_VALUE)) { CRC32COpen = true; }

        IsBigDataOnly = cfg.getBoolean(Constants.CS_US3_IS_BIGDATA_ONLY, false);

        UseMDS = cfg.getBoolean(Constants.CS_US3_USE_MDS , false);
        if (!UseMDS) { UseMDS = cfg.getBoolean(Constants.CS_UFILE_USE_MDS, false); }
        if (UseMDS) {
            MDSHost = cfg.get(Constants.CS_US3_MDS_HOST, "");
            if (MDSHost.isEmpty()) {
                MDSHost = cfg.get(Constants.CS_UFILE_MDS_HOST, "");
            }
            customZookeeperAddresses = cfg.get(Constants.CS_UFILE_MDS_ZOOKEEPER_ADDRS);
            if(customZookeeperAddresses == null){
                customZookeeperAddresses =cfg.get(Constants.CS_US3_MDS_ZOOKEEPER_ADDRS);
            }
            hdZookeeperAddresses = cfg.get(Constants.HD_ZOOKEEPER_ADDRS);
        }
        
        UseAsyncWIO = cfg.getBoolean(Constants.CS_US3_USE_ASYNC_WIO, false);
        if (!UseAsyncWIO) { UseAsyncWIO = cfg.getBoolean(Constants.CS_UFILE_USE_ASYNC_WIO, false); }
        if (UseAsyncWIO) {
            asyncWIOParallel = cfg.getInt(Constants.CS_US3_ASYNC_WIO_PARALLEL, 0);
            if (asyncWIOParallel == 0) {
                asyncWIOParallel = cfg.getInt(Constants.CS_UFILE_ASYNC_WIO_PARALLEL, Constants.DEFAULT_CS_ASYNC_WIO_PARALLEL);
            }
        }

        generateMD5 = cfg.getBoolean(Constants.CS_US3_GENERATE_MD5, false);
        ioTimeout = cfg.getInt(Constants.CS_UFILE_TIMEOUT, -1);
        if(ioTimeout <= 0){
            ioTimeout = cfg.getInt(Constants.CS_US3_TIMEOUT, -1);
        }
        if(ioTimeout <= 0){
            ioTimeout = Constants.DEFAULT_TIMEOUT;
        }

        retryTimes = cfg.getInt(Constants.CS_UFILE_RETRY_TIMES, -1);
        if(retryTimes < 0){
            retryTimes = cfg.getInt(Constants.CS_US3_RETRY_TIMES, -1);
        }
        if(retryTimes < 0 ){
            retryTimes = Constants.DEFAULT_MAX_TRYTIMES;
        }
    }
    public String getCustomZookeeperAddresses() {
        return customZookeeperAddresses;
    }
    public String getHdZookeeperAddresses() {
        return hdZookeeperAddresses;
    }
    public String getPublicKey() { return PublicKey; }

    public String getPrivateKey() { return PrivateKey; }

    public String getEndPoint() { return EndPoint; }

    public int getReadBufferSize() { return ReadBufferSize; }

    public int getMaxFileSizeUsePut() { return Constants.MAX_FILE_SIZE_USE_PUT; }

    public cn.ucloud.ufile.fs.LOGLEVEL getLogLevel() { return LogLevel; }

    public boolean getCRC32COpen() { return CRC32COpen; }

    public boolean getIsBigDataOnly() { return IsBigDataOnly; }

    public boolean isUseMDS() { return UseMDS; }

    public String getMDSHost() { return MDSHost; }  

    public void setMDSHost(String host) {MDSHost = host;}
    
    public boolean isUseAsyncWIO() { return UseAsyncWIO; }

    public int getAsyncWIOParallel() { return asyncWIOParallel; }

    public boolean isGenerateMD5() { return generateMD5; }

    public String toString() {
        return "\n1. "+Constants.CS_US3_ACCESS_KEY+":"+PublicKey + "\n"+
                "2. "+Constants.CS_US3_SECRET_KEY+":"+PrivateKey + "\n"+
                "3. "+Constants.CS_US3_ENDPOINT+EndPoint+"\n"+
                "4. "+"Maximum file size uploaded using PUT"+":"+MaxFileSizeUsePut+"\n"+
                "5. "+Constants.CS_US3_SOCKET_RECV_BUFFER+":"+ReadBufferSize+"\n"+
                "6. "+Constants.CS_US3_LOG_LEVEL_KEY+":"+LogLevel+"\n"+
                "7. "+Constants.CS_US3_IS_BIGDATA_ONLY+":"+IsBigDataOnly+"\n"+
                "8. "+Constants.CS_US3_USE_MDS+":"+UseMDS+"\n"+
                "9. "+Constants.CS_US3_MDS_HOST+":"+MDSHost+"\n"+
                "10."+Constants.CS_US3_USE_ASYNC_WIO+":"+UseAsyncWIO+"\n"+
                "11."+Constants.CS_US3_ASYNC_WIO_PARALLEL+":"+asyncWIOParallel+"\n"+
                "12."+Constants.CS_UFILE_TIMEOUT+":"+getIoTimeout()+"\n"+
                "13."+Constants.CS_UFILE_RETRY_TIMES+":"+getRetryTimes()+"\n"+
                "14."+Constants.CS_US3_GENERATE_MD5+":"+generateMD5+"\n";

    }
}
