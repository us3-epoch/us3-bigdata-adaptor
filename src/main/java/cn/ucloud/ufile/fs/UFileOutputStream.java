package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.api.object.PutStreamApi;
import cn.ucloud.ufile.api.object.multi.FinishMultiUploadApi;
import cn.ucloud.ufile.api.object.multi.MultiUploadInfo;
import cn.ucloud.ufile.api.object.multi.MultiUploadPartState;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.bean.base.BaseObjectResponseBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import cn.ucloud.ufile.fs.common.Crc32c;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.commons.codec.binary.Base64;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;

public class UFileOutputStream extends OutputStream {
    enum UploadMode {
        PUT,
        PART,
    };

    /** 用来管理内部上传Buff的类*/
    private class Block {
        /** 保存临时数据的地方*/
        private byte[] buf;

        /** 写偏移*/
        private int writeOffset;

        /** 写偏移*/
        private int bufLen;

        public Block(int bufSize) {
            this.buf = new byte[bufSize];
            this.bufLen = bufSize;
        }

        public Block() {}

        /**
         * 该操作保证一定能写入数据，除非buf已经写满
         * @param b
         * @param off
         * @param len
         * @return
         */
        public int write(byte[] b, int off, int len) {
            int canWrite = this.available();
            if (canWrite > 0) {
                int totalBytesRead = Math.min(canWrite, len);
                System.arraycopy(b, off, buf, writeOffset, totalBytesRead);
                this.writeOffset += totalBytesRead;
                return totalBytesRead;
            }

            return 0;
        }

        private int available() {
            return this.bufLen - writeOffset;
        }

        private int getWriteOffset() { return writeOffset; }

        /** help gc*/
        public void close() {
            buf = null;
        }

        public byte[] getBuf() { return buf; }

        private void reset() { writeOffset = 0; }
    };

    private class Uploader {
        private UfileObjectLocalAuthorization objAuthCfg;

        private ObjectConfig objCfg;

        private String bucket;

        private String key;

        /** 分片上传模式是否已经初始化*/
        private volatile boolean partIsInit = false;

        /** 分片初始化结构*/
        private MultiUploadInfo stat;

        /** 分片的ETag等相关信息*/
        List<MultiUploadPartState> partStates;

        /** 分片个数*/
        private int count;

        public Uploader(UfileObjectLocalAuthorization objAuthCfg,
                        ObjectConfig objCfg,
                        OSMeta osMeta) {
            this.objAuthCfg = objAuthCfg;
            this.objCfg = objCfg;
            this.bucket = osMeta.getBucket();
            this.key = osMeta.getKey();
        }

        public String getKey() { return key; }

        /** 对Block进行分片上传*/
        public synchronized void MultiPartWrite(Block b, String  mimeType) throws IOException {
            if (!partIsInit) {
                MultiPartInit(mimeType);
            }

            int len = b.getWriteOffset();
            if (len <= 0) return ;

            final int index = count++;
            /** 可支持重试3次上传*/
            UFileUtils.Debug(cfg.getLogLevel(), "[MultiPartWrite] part key:%s uploadId:%s index:%d \n", key, stat.getUploadId(), index);
            int tryCount = 0;
            Exception exception = null;
            /** 错误重试 */
            while (true) {
                try {
                    MultiUploadPartState partState = UfileClient.object(objAuthCfg, objCfg)
                            .multiUploadPart(stat, b.getBuf(), 0, b.getWriteOffset(), index)
                            .execute();
                    partStates.add(partState);
                    return;
                } catch (UfileClientException e) {
                    e.printStackTrace();
                    exception = e;
                } catch (UfileServerException e) {
                    e.printStackTrace();
                    exception = e;
                }

                try {
                    if (tryCount < Constants.PART_DEFAULT_MAX_TRYTIMES) {
                        Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                    }
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                if (tryCount >= Constants.PART_DEFAULT_MAX_TRYTIMES) {
                    break;
                }
                tryCount++;
            }

            throw UFileUtils.TranslateException(String.format("multi uploadid:%s key:%s part(%s) %s", stat.getUploadId(), bucket, key, mode.toString() ), key, exception);
        }

        public void MultiPartFinish(Map<String, String> userMeta) throws IOException {
            if (!partIsInit) {
                throw new IOException("Uploader.MultiPartFinish multipart upload instance is not initialize");
            }

            UFileUtils.Debug(cfg.getLogLevel(), "[MultiPartFinish] key:%s part uploadId:%s \n",key, stat.getUploadId());
            if (userMeta != null) {
                Iterator it = userMeta.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry item = (Map.Entry) it.next();
                    UFileUtils.Debug(cfg.getLogLevel(),"[MultiPartFinish] userMeta key:%s val:%s  \n",  (String)item.getKey(), (String)item.getValue());
                }
            }

            int tryCount = 0;
            Exception exception = null;
            /** 错误重试 */
            while (true) {
                try {
                    FinishMultiUploadApi api = UfileClient.object(objAuthCfg, objCfg)
                            .finishMultiUpload(stat, partStates)
                            .withMetaDatas(userMeta)
                            .withMetadataDirective("REPLACE");
                    api.execute();
                    return;
                } catch (UfileClientException e) {
                    e.printStackTrace();
                    exception = e;
                } catch (UfileServerException e) {
                    e.printStackTrace();
                    exception = e;
                }

                try {
                    if (tryCount < Constants.PART_DEFAULT_MAX_TRYTIMES) {
                        Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                    }
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                if (tryCount >= Constants.PART_DEFAULT_MAX_TRYTIMES) {
                    break;
                }
                tryCount++;
            }
            MultiPartAbort();
            throw UFileUtils.TranslateException(String.format("multi upload finish, key:%s uploadId:%s ", key,  stat.getUploadId()), key, exception);
        }

        private void MultiPartAbort() throws IOException {
            if (stat == null) { return;}
            UFileUtils.Debug(cfg.getLogLevel(), "[MultiPartAbort] key:%s part uploadId:%s", key, stat.getUploadId());
            int tryCount = 0;
            Exception exception = null;
            /** 错误重试 */
            while (true) {
                try {
                    BaseObjectResponseBean res = UfileClient.object(objAuthCfg, objCfg)
                            .abortMultiUpload(stat)
                            .execute();
                    return;
                } catch (UfileClientException e) {
                    e.printStackTrace();
                    exception = e;
                } catch (UfileServerException e) {
                    e.printStackTrace();
                    exception = e;
                }

                try {
                    if (tryCount < Constants.PART_DEFAULT_MAX_TRYTIMES) {
                        Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                    }
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                if (tryCount >= Constants.PART_DEFAULT_MAX_TRYTIMES) {
                    break;
                }
                tryCount++;
            }

            throw UFileUtils.TranslateException(String.format("part upload abort, uploadid:%s ", stat.getUploadId()), key, exception);
        }

        private synchronized void MultiPartInit(String mimeType) throws IOException {
            int tryCount = 0;
            Exception exception = null;
            /** 错误重试 */
            while (true) {
                try {
                    stat = UfileClient.object(objAuthCfg, objCfg)
                            .initMultiUpload(key, mimeType, bucket)
                            .execute();
                    partIsInit = true;
                    partStates = new ArrayList<>();
                    return;
                } catch (UfileClientException e) {
                    e.printStackTrace();
                    exception = e;
                } catch (UfileServerException e) {
                    e.printStackTrace();
                    exception = e;
                }

                try {
                    if (tryCount < Constants.PART_DEFAULT_MAX_TRYTIMES) {
                        Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (tryCount >= Constants.PART_DEFAULT_MAX_TRYTIMES) {
                    break;
                }
                tryCount++;
            }

            throw UFileUtils.TranslateException("part init ", key, exception);
        }

        public void Put(Block b, String mimeType, Map<String, String> userMeta) throws IOException {
            int tryCount = 0;
            Exception exception = null;
            /*if (userMeta != null) {
                Iterator it = userMeta.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry item = (Map.Entry) it.next();
                    UFileUtils.Debug(cfg.getLogLevel(),"[Put] userMeta key:%s val:%s ",  (String)item.getKey(), (String)item.getValue());
                }
            }*/
            /** 错误重试 */
            while (true) {
                ByteArrayInputStream stream;
                long streamLength = 0;
                if (b.getBuf() == null || b.getBuf().length == 0) {
                    stream = new ByteArrayInputStream(Constants.empytBuf, 0, 0);
                } else {
                    stream = new ByteArrayInputStream(b.getBuf(), 0, b.getWriteOffset());
                    streamLength = b.getWriteOffset();
                }

                try {

                    PutStreamApi api = UfileClient.object(objAuthCfg, objCfg)
                        //adapted for sdk2.6.6 
                        //.putObject(stream,mimeType)
                        .putObject(stream,streamLength,mimeType)
                        .withMetaDatas(userMeta)
                        .nameAs(key)
                        .toBucket(bucket)
                        //adapted for sdk2.6.6 
                        //.withVerifyMd5(false);
                        .withVerifyMd5(false, "");
                        api.execute();
                    return;
                } catch (UfileClientException e) {
                    e.printStackTrace();
                    exception = e;
                } catch (UfileServerException e) {
                    e.printStackTrace();
                    exception = e;
                }

                try {
                    if (tryCount < Constants.PUT_DEFAULT_MAX_TRYTIMES) {
                        Thread.sleep(tryCount*Constants.TRY_DELAY_BASE_TIME);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (tryCount >= Constants.PUT_DEFAULT_MAX_TRYTIMES) {
                    break;
                }
                tryCount++;
            }

            throw UFileUtils.TranslateException("put faild ", key, exception);
        }

        public synchronized void close() {
            if (partIsInit) {
                partStates = null;
                stat = null;
            }
        }

        private synchronized void setKey(String Key) {
            key = Key;
        }
    }

    /** 默认认为采用PUT上传*/
    private UploadMode mode = UploadMode.PUT;

    /** 缓存*/
    private Block block;

    /** 上传管理器*/
    private Uploader uploader;

    private Configure cfg;

    private FsPermission perm;

    private Crc32c crc32c;

    private String mimeType = Constants.UPLOAD_DEFAULT_MIME_TYPE;

    private boolean isClosed = false;

    private UFileFileSystem ufileFS = null;

    private final byte[] singleCharWrite = new byte[1];

    private long blockSize;

    private short replication;

    private boolean isNotifyMds = false;

    private long writed = 0;

    private MessageDigest md5;

    public UFileOutputStream(
                             UFileFileSystem ufileFS,
                             Configure cfg,
                             UfileObjectLocalAuthorization objAuthCfg,
                             ObjectConfig objCfg,
                             OSMeta osMeta,
                             FsPermission permission,
                             /** 该参数实际是都当做True */
                             boolean overwrite,
                             /** 目前没有意义 */
                             int bufferSize,
                             /** 目前没有意义 */
                             short replication,
                             /** 目前没有意义 */
                             long blockSize,
                             /** 目前没有意义 */
                             Progressable progress,
                             boolean needBuf) throws IOException {
        this.cfg = cfg;
        this.perm = permission;
        /** 目前限制死Block的大小为4MB*/
        if (needBuf) {
            this.block = new Block(cfg.getMaxFileSizeUsePut());
        } else {
            this.block = new Block();
        }
        this.uploader = new Uploader(objAuthCfg, objCfg, osMeta);

        if (cfg.getCRC32COpen()) {
            crc32c = new Crc32c();
        }

        if (cfg.isGenerateMD5()) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        this.ufileFS = ufileFS;
        this.blockSize = blockSize;
        this.replication = replication;
    }

    /**
     * 该方法需要实现
     * @param b
     * @throws IOException
     */
    @Override
    public void write(int b) throws IOException {
        singleCharWrite[0] = (byte)b;
        write(singleCharWrite, 0, 1);
    }

    /**
     * 覆盖父类实现，因为该流采用buffer机制
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b.length == 0 || len <= 0) { return; }
        //UFileUtils.Trace(cfg.getLogLevel(), "[write] key:%s off:%d len:%d", uploader.getKey(), off, len);
        writed+=len;
        internalWrite(b, off, len);
    }

    /**
     * 不对writed做操作
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    private void internalWrite(byte[] b, int off, int len) throws IOException {
        switch (mode) {
            case PUT:
                writeBlock(b, off, len);
                break;
            case PART:
                /** 该实现依赖于Block大小只有4MB的情况*/
                if (block.available() == 0) {
                    try {
                        uploader.MultiPartWrite(block, mimeType);
                    } catch (IOException e) {
                        uploader.MultiPartAbort();
                        isClosed = true;
                        throw UFileUtils.TranslateException(String.format("[UFileOutputStream.write] part write "), uploader.getKey(), e);
                    }
                    block.reset();
                }
                writeBlock(b, off, len);
        }
    }

    private void writeBlock(byte[] b, int off, int len) throws IOException {
        //UFileUtils.Trace(cfg.getLogLevel(), "[writeBlock] key:%s off:%d len:%d", uploader.getKey(), off, len);
        //UFileUtils.Debug(cfg.getLogLevel(), "[writeBlock] content:%s\n", new String(b));
        int written = block.write(b, off, len);
        if (0 == written) {
            mode = UploadMode.PART;
        } else {
            if (cfg.getCRC32COpen()) {
                crc32c.update(b, off, written);
            }

            if (md5 != null) {
                md5.update(b, off, written);
            }
        }

        if (written < len) {
            /** 证明一个block不能容纳该流的所有数据，所以得变成分片上传*/
            //UFileUtils.Debug(cfg.getLogLevel(), "[writeBlock] key:%s written:%d len:%d", uploader.getKey(), written, len);
            internalWrite(b, off+written, len-written);
        }
    }

    /**
     * 该方法的覆盖，是因为针对小文件做了优化，小文件会直接写到缓存中，
     * 当关闭该流时，才进行PUT上传操作，而不是一开始就采用分片上传。
     * 目前的成本就是每个OutPutStream会有一个4MB的buffer
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        UFileUtils.Debug(cfg.getLogLevel(), "[close] key:%s mode:%s", uploader.getKey(), mode.toString() );
        String objKey = uploader.getKey();
        Constants.ufileMetaStore.removeUFileFileStatus(objKey);
        int lastDelimiterIndex = objKey.lastIndexOf("/");
        if (lastDelimiterIndex != -1){
        String parent = objKey.substring(0, objKey.lastIndexOf("/")+1);
        //同样去掉父目录的缓存
        Constants.ufileMetaStore.removeUFileFileStatus(parent);
        }
        String HexCrc32c = null;
        if (crc32c != null ) {
            if (writed > 0) {
                HexCrc32c = Long.toHexString(crc32c.getValue());
                UFileUtils.Debug(cfg.getLogLevel(), "[close] key:%s hexCrc32:%s", uploader.getKey(), HexCrc32c);
            }
        }

        String Base64Md5 = null;
        if (md5 != null ) {
            if (writed > 0) {
                /*
                BASE64Encoder base64=new BASE64Encoder();
                Base64Md5 = base64.encode(md5.digest());
                */
                Base64Md5 = Base64.encodeBase64String(md5.digest());
                UFileUtils.Debug(cfg.getLogLevel(), "[close] key:%s base64md5:%s", uploader.getKey(), Base64Md5);
            }
        }

        Map<String, String> userMeta = ufileFS.extractUserMeta(
                ufileFS.getUsername(),
                Constants.superGroup,
                HexCrc32c,
                perm,
                blockSize,
                replication,
                Base64Md5
        );

        switch (mode) {
            case PUT:
                uploader.Put(block, mimeType, userMeta);
                break;
            case PART:
                /** 需要做一个Flush操作，防止block数据没有刷入 */
                try {
                    uploader.MultiPartWrite(block, mimeType);
                    uploader.MultiPartFinish(userMeta);
                } catch (IOException e) {
                    uploader.MultiPartAbort();
                    throw UFileUtils.TranslateException(String.format("[UFileOutputStream.close] part close "), uploader.getKey(), e);
                }
                break;
        }
        block.close();
        isClosed = true;

        if (cfg.isUseMDS()) {
            userMeta.put(Constants.CONTENT_LENGTH_FOR_NOTIFY, Long.toString(writed));
            MetadataService.Nodify(ufileFS, userMeta, this.uploader.bucket, this.uploader.key, mimeType);
        } else {
            UFileUtils.KeepListFileExistConsistency(ufileFS, uploader.getKey(), Constants.DEFAULT_MAX_TRYTIMES, true);
        }
    }

    /**
     * 修改写入流的文件类型，默认为application/octet-stream类型
     * @param mimeType 文件类型
     */
    public void setMimeType(String mimeType) { this.mimeType = mimeType; }
}
