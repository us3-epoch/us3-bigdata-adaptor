package cn.ucloud.ufile.fs.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class ThreadSafeSimpleDateFormat {
    class compositeSDF {
        private SimpleDateFormat sdf;
        /**
         * use 'yyyy';
         * reference: https://mp.weixin.qq.com/s?__biz=MzA3MjY1MTQwNQ==&mid=2649833072&idx=1&sn=9bab56642f6852ee28571d5ae4e1462f&chksm=871e9992b069108486c00afdc36e8b41047e3edf01d329ba86ff1e516374bfa879e0dffd2b73&mpshare=1&scene=1&srcid=0114GRWRw5SwXe0m80x4Hxyg&sharer_sharetime=1578998346940&sharer_shareid=8b90f1b90136441329f688379bb00065&rd2werd=1#wechat_redirect
         * 'YYYY' represent 'Week Year'
         * 'yyyy' represent 'date belong to some year'
         */
        public compositeSDF() {
            //sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.getDefault());
            //System.out.printf("local:%s\n", Locale.getDefault().toString());
            sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        }

        public synchronized Date parse(String source) throws ParseException {
            return sdf.parse(source);
        }

        public String format(Date date) {
            return sdf.format(date);
        }
    }

    private compositeSDF[] CSDF;

    private static int DEFAULT_HASH_BUCKET_SIZE = 4;

    public int hash(String key) {
        int hash = 0;
        byte[] byts = key.getBytes();
        for (int i = 0; i < byts.length; i++) {
            hash = (hash << 5) - hash + byts[i];
        }

        if (hash < 0) hash = Math.abs(hash);

        return hash;
    }

    public ThreadSafeSimpleDateFormat(int bucketSize) {
        CSDF = new compositeSDF[bucketSize];
        for (int i = 0; i < bucketSize; i++) {
            CSDF[i] = new compositeSDF();
        }
    }

    public ThreadSafeSimpleDateFormat() {
        this(DEFAULT_HASH_BUCKET_SIZE);
    }

    public Date parse(String source) throws ParseException {
        int idx = hash(source) % CSDF.length;
        return CSDF[idx].parse(source);
    }

    public String format(Date date) {
        return CSDF[0].format(date);
    }
}
