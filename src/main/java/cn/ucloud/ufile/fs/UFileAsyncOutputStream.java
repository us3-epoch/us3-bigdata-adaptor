package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.api.ApiError;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.api.object.PutStreamApi;
import cn.ucloud.ufile.api.object.multi.FinishMultiUploadApi;
import cn.ucloud.ufile.api.object.multi.MultiUploadInfo;
import cn.ucloud.ufile.api.object.multi.MultiUploadPartState;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.bean.UfileErrorBean;
import cn.ucloud.ufile.bean.base.BaseObjectResponseBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import cn.ucloud.ufile.fs.common.Crc32c;
import cn.ucloud.ufile.http.UfileCallback;

import org.apache.hadoop.fs.permission.FsPermission;
//import sun.misc.BASE64Encoder;
import org.apache.commons.codec.binary.Base64;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
//adapted for sdk2.6.6
//import com.squareup.okhttp.Request;
import okhttp3.Request;
/**
 * @Name: cn.ucloud.ufile.fs
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 18:30
 */

enum WriteMode {
    PUT,
    PART,
    FINISH,
}

/** 用来管理内部上传Buffer的类*/
class Extent extends UfileCallback<MultiUploadPartState> {
    private StorageLayer sl;
    /** 保存临时数据的地方*/
    private byte[] buf;
    /** 写偏移*/
    private int writeOffset = 0;

    private volatile boolean used = false;

    public Exception exc = null;

    private int partNumber = 0;

    public Extent(StorageLayer sl, int partNumber) {
        this.sl = sl;
        this.partNumber = partNumber;
    }

    /**
     * 该操作保证一定能写入数据，除非buf已经写满
     * @param b
     * @param off
     * @param len
     * @return
     */
    public int write(byte[] b, int off, int len) {
        int avai = this.available();
        if (avai > 0) {
            int totalBytesRead = Math.min(avai, len);
            System.arraycopy(b, off, buf, writeOffset, totalBytesRead);
            this.writeOffset += totalBytesRead;
            return totalBytesRead;
        }
        return 0;
    }

    public int available() {
        if (buf == null) this.buf = new byte[Constants.MAX_FILE_SIZE_USE_PUT];
        return this.buf.length - writeOffset;
    }

    public int length() { return writeOffset; }

    public int getPartNumber() { return partNumber; }

    public void setPartNumber(int partNumber) { this.partNumber = partNumber; }

    /** help gc*/
    public void close() {
        sl = null;
        buf = null;
    }

    public byte[] getBuffer() { return buf; }

    private void reset() { writeOffset = 0; }

    public boolean isUsed() { return used; }

    public void busy() { used = true;}

    public void idle() {
        reset();
        used = false;
    }

    @Override
    public void onResponse(MultiUploadPartState response) {
        sl.appendPartStat(response);
        this.idle();
        sl.unLock();
    }

    @Override
    public void onError(Request request, ApiError error, UfileErrorBean response) {
        if (response != null) {
            this.exc = new IOException(String.format("Mutil Part Upload UfileError, %s", response.getErrMsg()));
        } else if (error != null) {
            this.exc = new IOException(String.format("Mutil Part Upload ApiError, %s", error.getMessage()));
        } else {
            this.exc = new IOException("Unknown Error");
        }
        this.idle();
        sl.unLock();
    }
}

class StorageLayer{
    private UFileAsyncOutputStream afs;

    private UfileObjectLocalAuthorization objAuthCfg;

    private ObjectConfig objCfg;

    private String bucket;

    private String key;

    /** 分片初始化结构*/
    private volatile MultiUploadInfo stat;

    /** 分片的ETag等相关信息*/
    private List<MultiUploadPartState> partStates;

    private LOGLEVEL logLevel;

    private Extent[] extents;

    private volatile boolean blocked = false;

    public StorageLayer(UFileAsyncOutputStream afs,
                        UfileObjectLocalAuthorization auth,
                        ObjectConfig objCfg,
                        String bucket,
                        String key,
                        LOGLEVEL loglevel,
                        int paralle) {
        this.afs = afs;
        this.objAuthCfg = auth;
        this.objCfg = objCfg;
        this.bucket = bucket;
        this.key = key;
        this.logLevel = loglevel;
        this.extents = new Extent[paralle];
    }

    public StorageLayer(UFileAsyncOutputStream afs,
                        UfileObjectLocalAuthorization auth,
                        ObjectConfig objCfg,
                        String bucket,
                        String key,
                        LOGLEVEL loglevel) {
        this(afs, auth, objCfg, bucket, key, loglevel, Constants.DEFAULT_CS_ASYNC_WIO_PARALLEL);
    }

    public void Put(Extent ext, String mimeType, Map<String, String> userMeta) throws IOException {
        int retryCount =1;
        while(true){
        try {
            ByteArrayInputStream stream;
            long streamLength = 0;
            if (ext == null || ext.getBuffer() == null || ext.getBuffer().length == 0) {
                stream = new ByteArrayInputStream(Constants.empytBuf, 0, 0);
            } else {
                stream = new ByteArrayInputStream(ext.getBuffer(), 0, ext.length());
                streamLength = ext.length();
            }
            PutStreamApi api = UfileClient.object(objAuthCfg, objCfg)
                        //adapted for sdk2.6.6
                        //.putObject(emptyStream, mimeType)
                        .putObject(stream, streamLength, mimeType)
                        .withMetaDatas(userMeta)
                        .nameAs(key)
                        .toBucket(bucket)
                        //adapted for sdk2.6.6
                        //.withVerifyMd5(false);
                        .withVerifyMd5(false, "");
            api.execute();
            return;
        } catch (UfileClientException e) {
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException("StorageLayer.Put ", key, e);
        } catch (UfileServerException e) {
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException("StorageLayer.Put ", key, e);
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }
    }
    }

    public void InitPart(String mimeType) throws IOException {
        UFileUtils.Debug(logLevel, "[InitPart] part key:%s mimeType:%s", key, mimeType);
        stat = new MultiUploadInfo();
        int retryCount = 1;
        while(true){
        try {
            stat = UfileClient.object(objAuthCfg, objCfg)
                    .initMultiUpload(key, mimeType, bucket)
                    .execute();
            partStates = new ArrayList<>();
            //stat.setUseHTTP2(true);
            return;
        } catch (Exception e) {
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException("StorageLayer.InitPart ", key, e);
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }
    }
    }

    public void DoPart(Extent ext, String mimeType) throws IOException {
        //UFileUtils.Debug(logLevel, "[DoPart] part key:%s partNumber:%d", key, ext.getPartNumber());
        if (stat == null) {
            InitPart(mimeType);
        }

        int len = ext.length();
        if (len <= 0) return ;
        //UFileUtils.Debug(logLevel, "[DoPart] part key:%s uploadId:%s partNumber:%d length:%d", key, stat.getUploadId(), ext.getPartNumber(), len);
        ext.busy();

        //UFileUtils.Debug(logLevel, "[DoPart] use http2:%b", stat.isUseHTTP2());
        UfileClient.object(objAuthCfg, objCfg)
                .multiUploadPart(stat, ext.getBuffer(), 0, ext.length(), ext.getPartNumber()).executeAsync(ext);
    }

    public void DonePart(Map<String, String> userMeta) throws IOException {
        //UFileUtils.Debug(logLevel, "[DonePart] key:%s part uploadId:%s",key, stat.getUploadId());
        //if (userMeta != null) {
        //    Iterator it = userMeta.entrySet().iterator();
        //    while(it.hasNext()) {
        //        Map.Entry item = (Map.Entry) it.next();
        //        UFileUtils.Debug(logLevel,"[DonePart] userMeta key:%s val:%s  \n",  (String)item.getKey(), (String)item.getValue());
        //    }
        //}
        int retryCount = 1;
        while(true){
        try {
            FinishMultiUploadApi api = UfileClient.object(objAuthCfg, objCfg)
                    .finishMultiUpload(stat, partStates)
                    .withMetaDatas(userMeta)
                    .withMetadataDirective("REPLACE");
            api.execute();
            return;
        } catch (Exception e) {
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException("StorageLayer.UndoPart ", key, e);
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }
    }
    }

    public void UndoPart() throws IOException {
        UFileUtils.Debug(logLevel, "[UndoPart] key:%s", key);
        if (partStates == null) return;
        UFileUtils.Debug(logLevel, "[UndoPart] key:%s part uploadId:%s", key, stat.getUploadId());
        int retryCount = 1;
        while(true){
        try {
            BaseObjectResponseBean res = UfileClient.object(objAuthCfg, objCfg)
                    .abortMultiUpload(stat)
                    .execute();
            return;
        } catch (Exception e) {
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException(String.format("StorageLayer.UndoPart, uploadid:%s ", stat.getUploadId()), key, e);
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }
    }
    }

    public synchronized void appendPartStat(MultiUploadPartState state) {
            partStates.add(state);
    }

    public synchronized void Lock(int timeOut) {
        blocked = true;
        while (blocked) {
            try {
                this.wait(timeOut);
                blocked = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void unLock() {
        if (blocked) {
            blocked = false;
            this.notify();
        }
    }

    /**
     * 这里会阻塞检测是否有Extent出现过失败
     * @return
     */
    public Exception checkException() {
        int i = 0;
        for (; i < extents.length; ) {
            if (extents[i] == null) {
                return null;
            }

            if (!extents[i].isUsed()) {
                if (extents[i].exc != null) {
                    return extents[i].exc;
                }
            } else {
                this.Lock(20);
                continue;
            }
            i++;
        }
        return null;
    }

    public Extent allocateExtent(int partNumber) {
        int loopTime=0;
        do {
            for (int i = 0; i < extents.length; i++) {
                if (extents[i] == null) {
                    extents[i] = new Extent(this, partNumber);
                    return extents[i];
                }

                if (!extents[i].isUsed()) {
                    extents[i].setPartNumber(partNumber);
                    return extents[i];
                }
            }
            loopTime++;
            if (loopTime >= 3) {
                this.Lock(20);
                loopTime = 0;
            }
        } while(true);
    }

    public String getKey() { return key;}
    public String getBucket() { return bucket;}
    public void close() {
        this.partStates = null;
        this.stat = null;
        for (Extent ex: this.extents) {
            if (ex != null) ex.close();
        }
    }

    public ObjectConfig getObjCfg() { return objCfg; }
}

public class UFileAsyncOutputStream extends OutputStream {
    public  UFileFileSystem  fs;
    /** 默认认为采用PUT上传*/
    private WriteMode mode = WriteMode.PUT;

    private LOGLEVEL logLevel;

    private FsPermission perm;

    private Crc32c crc32c;

    private String mimeType = Constants.UPLOAD_DEFAULT_MIME_TYPE;

    private byte[] singleCharWrite = new byte[1];

    private StorageLayer sl;

    private Extent ext = null;

    private int partNumber = 0;

    private boolean closed = false;

    private long blockSize;

    private long writed;

    private MessageDigest md5;

    public UFileAsyncOutputStream(
            UFileFileSystem ufs,
            Configure cfg,
            UfileObjectLocalAuthorization objAuthCfg,
            ObjectConfig objCfg,
            FsPermission permission,
            OSMeta osMeta,
            String mimeType,
            long blockSize) throws IOException {
        this.fs = ufs;
        this.logLevel = cfg.getLogLevel();
        this.perm = permission;
        this.blockSize = blockSize;
        if (cfg.getCRC32COpen()) crc32c = new Crc32c();
        if (cfg.isGenerateMD5()) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        if (!mimeType.equals("")) this.mimeType = mimeType;
        sl = new StorageLayer(this, objAuthCfg, objCfg, osMeta.getBucket(), osMeta.getKey(), logLevel, cfg.getAsyncWIOParallel());
    }

    /**
     * 该方法需要实现
     * @param b
     * @throws IOException
     */
    @Override
    public synchronized void write(int b) throws IOException {
        if (singleCharWrite == null) singleCharWrite = new byte[1];
        singleCharWrite[0] = (byte)b;
        write(singleCharWrite, 0, 1);
    }

    private void keepExtentExist() throws IOException {
        if (ext == null) {
            ext = sl.allocateExtent(partNumber);
            partNumber++;
            if (ext == null) {
                throw new IOException("Can't allocate Extent");
            }

            if (ext.exc != null) {
                throw  new IOException(ext.exc);
            }
        }
    }

    /**
     * 归还Extent，并把当前用的置为null
     * @return
     */
    private Extent dropExtent() {
        Extent tmp = ext;
        ext = null;
        return tmp;
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
        writed+=len;
        keepExtentExist();
        writeLoop(b, off, len);
    }

    public void writeLoop(byte[] b, int off, int len) throws IOException {
        if (b.length == 0 || len <= 0) { return; }
        //UFileUtils.Trace(logLevel, "[UFileAsyncOutputStream.write] key:%s off:%d len:%d", sl.getKey(), off, len);
        switch (mode) {
            case PUT:
                writeExtent(b, off, len);
                break;
            case PART:
                /** 该实现依赖于Extent大小只有4MB的情况*/
                if (ext.available() == 0) {
                    try {
                        sl.DoPart(this.dropExtent(), mimeType);
                        // 切换合适的block
                        keepExtentExist();
                    } catch (IOException e) {
                        e.printStackTrace();
                        /** init failure */
                        closed = true;
                        sl.UndoPart();
                        sl.close();
                        throw UFileUtils.TranslateException(String.format("[UFileAsyncOutputStream.write] part write"), sl.getKey(), e);
                    } catch (Exception e) {
                        //UFileUtils.Debug(logLevel, "[writeLoop] key:%s ", sl.getKey());
                        //e.printStackTrace();
                        throw UFileUtils.TranslateException(String.format("[UFileAsyncOutputStream.write] part write exception"), sl.getKey(), e);
                    }
                }
                writeExtent(b, off, len);
        }
    }

    private void writeExtent(byte[] b, int off, int len) throws IOException {
        //UFileUtils.Trace(logLevel, "[writeExtent] key:%s off:%d len:%d", sl.getKey(), off, len);
        //UFileUtils.Debug(logLevel, "[writeExtent] content:%s\n", new String(b));
        int written = ext.write(b, off, len);
        if (0 == written) {
            mode = WriteMode.PART;
        } else {
            if (crc32c != null) {
                crc32c.update(b, off, written);
            }

            if (md5 != null) {
                md5.update(b, off, written);
            }
        }
        if (written < len) {
            //UFileUtils.Debug(logLevel, "[writeExtent] key:%s written:%d len:%d", sl.getKey(), written, len);
            writeLoop(b, off+written, len-written);
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
        if (closed) {
            return;
        }

        UFileUtils.Debug(logLevel, "[UFileAsyncOutputStream.close] key:%s mode:%s", sl.getKey(), mode.toString());
        String objKey = sl.getKey();
        int lastDelimiterIndex = objKey.lastIndexOf("/");
        if (lastDelimiterIndex != -1){
        }
        String HexCrc32c = null;
        if (crc32c != null) {
            if (writed > 0) {
                HexCrc32c = Long.toHexString(crc32c.getValue());
                UFileUtils.Debug(logLevel, "[UFileAsyncOutputStream.close] key:%s hexCrc32:%s", sl.getKey(), HexCrc32c);
            }
        }

        String Base64Md5 = null;
        if (md5 != null ) {
            if (writed > 0) {
                /*
                Base64Encoder base64 = new Base64Encoder();
                Base64Md5 = base64.encode(md5.digest());
                */
                Base64Md5 = Base64.encodeBase64String(md5.digest());
                UFileUtils.Debug(logLevel, "[close] key:%s base64md5:%s", sl.getKey(), Base64Md5);
            }
        }

        Map<String, String> userMeta = fs.extractUserMeta(
                fs.getUsername(),
                Constants.superGroup,
                HexCrc32c,
                perm,
                blockSize,
                (short) 1,
                Base64Md5
        );

        closed = true;
        switch (mode) {
            case PUT:
                sl.Put(ext, mimeType, userMeta);
                break;
            case PART:
                try {
                    /** 需要做一个Flush操作，防止Extent数据没有刷入 */
                    sl.DoPart(this.dropExtent(), mimeType);
                    keepExtentExist();

                    Exception exc = sl.checkException();
                    if (exc != null) {
                        sl.UndoPart();
                        throw new IOException(exc);
                    }

                    sl.DonePart(userMeta);
                } catch (IOException e) {
                    sl.UndoPart();
                    throw UFileUtils.TranslateException(String.format("[UFileMultiThreadOutputStream.close] part close "), sl.getKey(), e);
                } finally {
                    sl.close();
                }
                break;
        }

        // 通知MDS
        if (fs.getCfg().isUseMDS()) {
            userMeta.put(Constants.CONTENT_LENGTH_FOR_NOTIFY, Long.toString(writed));
            MetadataService.Nodify(fs, userMeta, this.sl.getBucket(), this.sl.getKey(), mimeType);
        } else {
            UFileUtils.KeepListFileExistConsistency(fs, sl.getKey(), Constants.DEFAULT_MAX_TRYTIMES, true);
        }
    }
}
