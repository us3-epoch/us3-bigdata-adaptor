package cn.ucloud.ufile.fs.tools;

/**
 * @Name: cn.ucloud.ufile.fs.tools
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 10:52
 */

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
import cn.ucloud.ufile.fs.Constants;
import cn.ucloud.ufile.fs.common.Crc32c;
import cn.ucloud.ufile.http.UfileCallback;
//adapted for sdk2.6.6
//import com.squareup.okhttp.Request;
import okhttp3.Request;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Name: cn.ucloud.ufile.fs
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 18:30
 */

enum LOGLEVEL { TRACE, DEBUG, INFO, ERROR }

enum WriteMode {
    PUT,
    PART,
    FINISH,
}

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
/** ????????????????????????Buffer??????*/
class Extent extends UfileCallback<MultiUploadPartState> {
    private StorageLayer sl;
    /** ???????????????????????????*/
    private byte[] buf;
    /** ?????????*/
    private int writeOffset = 0;

    private volatile boolean used = false;

    public Exception exc = null;

    private int partNumber = 0;

    public Extent(StorageLayer sl, int partNumber) {
        this.sl = sl;
        this.partNumber = partNumber;
    }

    /**
     * ?????????????????????????????????????????????buf????????????
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
        if (buf == null) this.buf = new byte[4*1024*1024];
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

    /** ?????????????????????*/
    private volatile MultiUploadInfo stat;

    /** ?????????ETag???????????????*/
    private List<MultiUploadPartState> partStates;

    private LOGLEVEL logLevel;

    private Extent[] extents = new Extent[12];

    private volatile boolean blocked = false;

    public StorageLayer(UFileAsyncOutputStream afs,
                        UfileObjectLocalAuthorization auth,
                        ObjectConfig objCfg,
                        String bucket,
                        String key,
                        LOGLEVEL loglevel, int parallel) {
        this.afs = afs;
        this.objAuthCfg = auth;
        this.objCfg = objCfg;
        this.bucket = bucket;
        this.key = key;
        this.logLevel = loglevel;
        this.extents = new Extent[parallel];
    }

    public void Put(Extent ext, String mimeType, Map<String, String> userMeta) throws IOException {
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
        } catch (UfileServerException e) {
            e.printStackTrace();
        }
    }

    public void InitPart(String mimeType) throws IOException {
        //UFileUtils.Debug(logLevel, "[InitPart] part key:%s mimeType:%s", key, mimeType);
        stat = new MultiUploadInfo();
        try {
            stat = UfileClient.object(objAuthCfg, objCfg)
                    .initMultiUpload(key, mimeType, bucket)
                    .execute();
            partStates = new ArrayList<>();
            //stat.setUseHTTP2(true);
            return;
        } catch (Exception e) {
            e.printStackTrace();
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

        try {
            FinishMultiUploadApi api = UfileClient.object(objAuthCfg, objCfg)
                    .finishMultiUpload(stat, partStates)
                    .withMetaDatas(userMeta)
                    .withMetadataDirective("REPLACE");
            api.execute();
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void UndoPart() throws IOException {
        //UFileUtils.Debug(logLevel, "[UndoPart] key:%s", key);
        if (partStates == null) return;
        //UFileUtils.Debug(logLevel, "[UndoPart] key:%s part uploadId:%s", key, stat.getUploadId());
        try {
            BaseObjectResponseBean res = UfileClient.object(objAuthCfg, objCfg)
                    .abortMultiUpload(stat)
                    .execute();
            return;
        } catch (Exception e) {
            e.printStackTrace();
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
     * ??????????????????????????????Extent???????????????
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
    public void close() {
        this.partStates = null;
        this.stat = null;
        for (Extent ex: this.extents) {
            if (ex != null) ex.close();
        }
    }
}

class UFileAsyncOutputStream {
    /** ??????????????????PUT??????*/
    private WriteMode mode = WriteMode.PUT;

    private LOGLEVEL logLevel;

    private Crc32c crc32c;

    private String mimeType = "application/octet-stream";

    private byte[] singleCharWrite = new byte[1];

    private StorageLayer sl;

    private Extent ext = null;

    private int partNumber = 0;

    private boolean closed = false;

    public UFileAsyncOutputStream(
            UfileObjectLocalAuthorization objAuthCfg,
            ObjectConfig objCfg,
            OSMeta osMeta,
            String mimeType,
            int parallel) {
        if (!mimeType.equals("")) this.mimeType = mimeType;

        sl = new StorageLayer(this, objAuthCfg, objCfg, osMeta.getBucket(), osMeta.getKey(), logLevel, parallel);
    }

    /**
     * ?????????????????????
     * @param b
     * @throws IOException
     */
 //   @Override
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
     * ??????Extent???????????????????????????null
     * @return
     */
    private Extent dropExtent() {
        Extent tmp = ext;
        ext = null;
        return tmp;
    }

    /**
     * ???????????????????????????????????????buffer??????
     * @param b
     * @param off
     * @param len
     * @throws IOException
     */
    //@Override
    public void write(byte[] b, int off, int len) throws IOException {
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
                /** ??????????????????Extent????????????4MB?????????*/
                if (ext.available() == 0) {
                    try {
                        sl.DoPart(this.dropExtent(), mimeType);
                        // ???????????????block
                        keepExtentExist();
                    } catch (IOException e) {
                        e.printStackTrace();
                        /** init failure */
                        closed = true;
                        sl.UndoPart();
                        sl.close();
                        e.printStackTrace();
                    } catch (Exception e) {
                        //UFileUtils.Debug(logLevel, "[writeLoop] key:%s ", sl.getKey());
                        e.printStackTrace();
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
        } else if (crc32c != null) {
            crc32c.update(b, off, written);
        }
        if (written < len) {
            //UFileUtils.Debug(logLevel, "[writeExtent] key:%s written:%d len:%d", sl.getKey(), written, len);
            writeLoop(b, off+written, len-written);
        }
    }

    /**
     * ????????????????????????????????????????????????????????????????????????????????????????????????
     * ??????????????????????????????PUT?????????????????????????????????????????????????????????
     * ???????????????????????????OutPutStream????????????4MB???buffer
     * @throws IOException
     */
    //@Override
    public void close() throws IOException {
        if (closed) { return; }

        //UFileUtils.Debug(logLevel, "[UFileAsyncOutputStream.close] key:%s mode:%s", sl.getKey(), mode.toString());
        closed = true;
        switch (mode) {
            case PUT:
                sl.Put(ext, mimeType, null);
                break;
            case PART:
                try {
                    /** ???????????????Flush???????????????Extent?????????????????? */
                    sl.DoPart(this.dropExtent(), mimeType);
                    keepExtentExist();

                    Exception exc = sl.checkException();
                    if (exc != null) {
                        sl.UndoPart();
                        throw new IOException(exc);
                    }

                    sl.DonePart(null);
                } catch (IOException e) {
                    sl.UndoPart();
                    e.printStackTrace();
                } finally {
                    sl.close();
                }
                break;
        }
    }
}

public class Http2 {
    enum Cmd {Delete, PUT};

    public static Http2.Cmd cmd = Http2.Cmd.PUT;

    public static String protocal = "http";

    public static String bucket = null;

    public static String key = null;

    public static String endpoint = null;

    public static String acckey = null;

    public static String seckey = null;

    public static long size = 0;

    public static int parallel = 12;

    public static void displayUsage() {
        String usage = "Usage: http2 <options>\n" +
                "Options:\n\t" +
                "[-cmd delete|put\n\t" +
                "[-mode http|https]\n\t" +
                "[-bucket <bucket>]\n\t" +
                "[-key <key>]\n\t" +
                "[-endpoint <endpoint>]\n\t" +
                "[-acckey <access key>]\n\t" +
                "[-seckey <secret key>]\n\t" +
                "[-parallel <parallel >]\n\t" +
                "[-size <number MB>]\n\t";
        System.out.println(usage);
        System.exit(-1);
    }

    public static void parseInputs(String[] args) {
        if (args.length == 0) {
            displayUsage();
        }
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("-cmd")) {
                ++i;
                switch (args[i]) {
                    case "delete":
                        cmd = Cmd.Delete;
                        break;
                    case "put":
                        cmd = Cmd.PUT;
                        break;
                    default:
                        displayUsage();
                }
            } else if (args[i].equals("-mode")) {
                ++i;
                protocal = args[i];
            } else if (args[i].equals("-bucket")) {
                ++i;
                bucket = args[i];
            } else if (args[i].equals("-key")) {
                ++i;
                key = args[i];
            } else if (args[i].equals("-endpoint")) {
                ++i;
                endpoint = args[i];
            } else if (args[i].equals("-acckey")) {
                ++i;
                acckey = args[i];
            } else if (args[i].equals("-size")) {
                ++i;
                System.out.printf("size:%s\n", args[i]);
                size = Long.parseLong(args[i])*1024*1024;
            } else if (args[i].equals("-parallel")) {
                ++i;
                System.out.printf("parallel:%s\n", args[i]);
                parallel = Integer.parseInt(args[i]);
            } else if (args[i].equals("-seckey")) {
                ++i;
                seckey = args[i];
            }
        }
    }

    public static void main(String[] args) {
        parseInputs(args);
        byte[] buffer = new byte[65535];
        buffer[0] = 'w';
        buffer[1] = 'h';
        buffer[2] = 'y';
        buffer[3] = ' ';
        System.out.printf("buffer length:%d\n", buffer.length);
        UfileObjectLocalAuthorization authCfg = new UfileObjectLocalAuthorization(acckey, seckey);
        ObjectConfig objCfg = new ObjectConfig(protocal+"://"+bucket+"."+endpoint);
        OSMeta osMeta = new OSMeta(bucket, key);
        UFileAsyncOutputStream out = new UFileAsyncOutputStream(authCfg, objCfg, osMeta, "", parallel);

        Instant begin = Instant.now();
        long count = 0;
        do {
            try {
              out.write(buffer, 0, buffer.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count += buffer.length;
        } while (count < size);

        System.out.printf("write over, count:%d\n", count);
        try {
            out.close();
            System.out.printf("close\n");
            Instant end = Instant.now();
            System.out.printf("taken %d ms\n",Duration.between(begin, end).toMillis());
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
