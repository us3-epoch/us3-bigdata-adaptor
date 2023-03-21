package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.auth.UfileAuthorizationException;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.auth.sign.UfileSignatureException;
import cn.ucloud.ufile.bean.DownloadStreamBean;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileParamException;
import cn.ucloud.ufile.exception.UfileServerException;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

public class UFileInputStream extends FSInputStream {
    /**
     * true 表明该流处于未开启状态
     * false ..........关闭...
     */
    private volatile boolean closed = true;

    private Configure cfg;

    private UfileObjectLocalAuthorization objAuthCfg;

    private ObjectConfig objCfg;

    private String bucket;

    private String key;

    private InputStream inputStream = null;

    private long readPos;

    private long skipPos;

    private long openTime;

    private long ireadSum;

    /** 当seek的范围除了超过了本地缓存以外,还超过了8MB则需要进行重新打开流,这样可以避免长时间读取流而提高性能*/
    private static long reopenMaxSeekLen = 0x800000;

    /** 暂时设置成 24小时 理论上一个文件不可能在一个Task中处理超过这么久*/
    private int expiresDuration = 24 * 60 * 60;

    public UFileInputStream(Configure cfg,
                            UfileObjectLocalAuthorization objAuthCfg,
                            ObjectConfig objCfg,
                            String bucket,
                            String key,
                            long contentLength) {
        this.cfg = cfg;
        this.objAuthCfg = objAuthCfg;
        this.objCfg = objCfg;
        this.bucket = bucket;
        this.key = key;
        this.readPos = 0;
        this.skipPos = 0;
        this.openTime = System.currentTimeMillis();
    }

    /**
     * 考虑到性能问题，这里只做标记，延迟流打开
     * @param pos
     * @throws IOException
     */
    @Override
    public synchronized void seek(long pos) throws IOException {
        this.skipPos = pos;
    }

    /**
     * iseek 负责确保在正确的位置上进行读，同时保证流是打开的
     * @throws IOException
     */
    private synchronized void iseek() throws IOException {
        if (skipPos == readPos) {
            if (closed) reopen(readPos);
            return;
        }

        if (readPos < skipPos) {
            /** 该情况证明需要往后seek*/
            if (closed) {
                /** 如果没有打开则从seek的位置打开*/
                reopen(skipPos);
                return;
            } else {
                if (!closed && (readPos + inputStream.available() + reopenMaxSeekLen >= skipPos)) {
                    /** 证明缓存里还有数据可以消费, 只需要对inputstream进行seek*/
                    long sum = 0;
                    long needSkipLen = skipPos-readPos;
                    while (sum < needSkipLen) {
                        long n = inputStream.skip(needSkipLen-sum);
                        sum += n;
                    }
                    readPos += sum;
                    return;
                }
            }
        }
        reopen(skipPos);
    }

    @Override
    public long getPos() throws IOException {
        /** 因为做了延迟skip操作，所以以skipPos操作 **/
        return skipPos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized int read() throws IOException {
        iseek();
        try {
            int n = inputStream.read();
            readPos++;
            skipPos++;
            ireadSum += 1;
            return n;
        } catch (IOException e){
            UFileUtils.Error(cfg.getLogLevel(), String.format("[UFileInputStream.read] failed at readPos:%d," +
                    " reopen stream and retry", readPos));
            reopen(skipPos);
            int n = inputStream.read();
            readPos++;
            skipPos++;
            ireadSum += 1;
            return n;
        }
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
            throws IOException {
        if (len == 0) {
            return 0;
        }
        iseek();
        int readSum = 0;
        boolean isOver = false;
        int count;
        while (!isOver && (readSum < len)) {
            /** 如果没有显示标志结束，而且buf没有读满*/
            try {
                Random rand =new Random(25);
                int i;
                i=rand.nextInt(100);
                if (i > 50){
                    throw new IOException();
                }
                count = inputStream.read(buf, off, len - readSum);
            } catch (IOException e){
                UFileUtils.Error(cfg.getLogLevel(), String.format("[UFileInputStream.read] failed at readPos:%d, offset:%d length:%d, error: %s" +
                        " reopen stream and retry", readPos, off, len, e.getMessage()));
                reopen(readPos);
                count = inputStream.read(buf, off, len - readSum);
            }
            switch (count) {
                case -1:
                    if (readSum == 0) {
                        /** 证明一开始读，流就显示已经碰到EOF，那么返回也要为-1，让上层感知到流已经结束了*/
                        readSum = -1;
                    }
                    /** 有可能循环读了几次，流才碰到EOF，那么返回应该是已经读到的字节数，让上层感知到流结束是通过下一次读感知*/
                case 0:
                    /** 流已经消费完毕，或者没有数据*/
                    isOver = true;
                    break;
                default:
                    off += count;
                    readSum += count;
                    readPos += count;
                    skipPos += count;
                    ireadSum += count;
            }
        }
        return readSum;
    }

    /**
     * 从文件指定位置开始读取文件
     * @param pos 文件的起始偏移读取位置
     * @throws IOException
     */
    private synchronized void reopen(long pos) throws IOException {
        if (!closed) {
            /** 如果之前有打开的流，需要先关闭*/
            close();
        }

        int tryCount = 1;
        Exception exception = null;
        while (true) {
            try {
                String url = UfileClient.object(objAuthCfg, objCfg)
                        .getDownloadUrlFromPrivateBucket(key, bucket, expiresDuration)
                        .createUrl();
                DownloadStreamBean response = UfileClient.object(objAuthCfg, objCfg)
                        .getStream(url).withinRange(pos, 0)
                        .execute();
                inputStream = new BufInputStream(cfg.getLogLevel(), response.getInputStream(), cfg.getReadBufferSize());
                readPos = pos;
                skipPos = pos;
                closed = false;
                return;
            } catch (UfileParamException e) {
                e.printStackTrace();
                throw UFileUtils.TranslateException("[UFileInputStream.reopen] param error ", key, e);
            } catch (UfileAuthorizationException e) {
                e.printStackTrace();
                throw UFileUtils.TranslateException("[UFileInputStream.reopen] authorization faild ", key, e);
            } catch (UfileSignatureException e) {
                e.printStackTrace();
                throw UFileUtils.TranslateException("[UFileInputStream.reopen] signature faild ", key, e);
            } catch (UfileClientException e) {
                e.printStackTrace();
                exception = e;
            } catch (UfileServerException e){
                if (e.getErrorBean().getResponseCode() == 416){
                    inputStream = new InputStream() {
                        @Override
                        public int read() throws IOException {
                            return -1;
                        }
                    };
                    readPos = pos;
                    skipPos = pos;
                    closed = false;
                    return;
                }
            }

            try {
                if (tryCount < Constants.GET_DEFAULT_MAX_TRYTIMES) {
                    Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                }
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            if (tryCount >= Constants.GET_DEFAULT_MAX_TRYTIMES) {
                break;
            }
            tryCount++;
        }

        throw UFileUtils.TranslateException("[UFileInputStream.reopen] faild ", key, exception);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (inputStream != null) inputStream.close();
        } finally {
            closed = true;
            inputStream = null;
            readPos = 0;
            skipPos = 0;
            ireadSum = 0;
            this.openTime = System.currentTimeMillis();
            //printTrack();
        }
    }

    public void printTrack(){
        java.util.Map<Thread, StackTraceElement[]> ts = Thread.getAllStackTraces();
        StackTraceElement[] ste = ts.get(Thread.currentThread());
        for (StackTraceElement s : ste) {
            UFileUtils.Info(cfg.getLogLevel(),"[UFileInputStream.printTrack][name:%s] %s", this.toString(), s.toString());
        }
    }
}
