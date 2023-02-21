package cn.ucloud.ufile.fs;

//import java.util.Hashtable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 该类用于缓存加速一些读取操作，如HEAD等
 */
public class UFileMetaStore {
    /** cache UFileFileStatus用 */
    private ConcurrentHashMap<String, UFileFileStatus>[] hsUFS;

    private int clearThreshold = 0;

    private static int DEFAULT_HASH_BUCKET_SIZE = 8;
    /**
     * 带hash桶大小构造meta store
     * @param hashBucketSize hash桶大小
     */
    public UFileMetaStore(int hashBucketSize) {
        if (hashBucketSize <= 0) hashBucketSize = UFileMetaStore.DEFAULT_HASH_BUCKET_SIZE;
        hsUFS = new ConcurrentHashMap[hashBucketSize];
        for (int i = 0; i < hashBucketSize; i++ )
        {
            hsUFS[i] = new ConcurrentHashMap<>();
        }
    }

    public UFileMetaStore() { this(0); }

    public void clean() {
        for (int i = 0; i < hsUFS.length; i++ ) { hsUFS[i].clear(); }
    }

    public int hash(String key) {
        clearThreshold++;
        if (clearThreshold > 128) { clean(); }
        int hash = 0;
        if (Constants.useSelfDefHashCode) {
            byte[] byts = key.getBytes();
            for (int i = 0; i < byts.length; i++) {
                hash = (hash << 5) - hash + byts[i];
            }
        } else hash = key.hashCode();

        if (hash < 0) hash = Math.abs(hash);
        return hash;
    }

    public void putUFileFileStatus(Configure cfg, String key, UFileFileStatus ufs) {
        int idx = hash(key) % hsUFS.length;
        //UFileUtils.Debug(cfg.getLogLevel(), "[putUFileFileStatus] idx:%d key:%s", idx, key);
        hsUFS[idx].put(key, ufs);
    }

    public void putUFileFileStatusWithTimeout(Configure cfg, String key, UFileFileStatus ufs, int timeout) {
        putUFileFileStatus(cfg, key, ufs);
        //UFileUtils.Debug(cfg.getLogLevel(), "[putUFileFileStatusWithTimeout] key:%s", key);
    }

    public UFileFileStatus removeUFileFileStatus(String key) {
        int idx = hash(key) % hsUFS.length;
        //UFileUtils.Debug(cfg.getLogLevel(), "[removeUFileFileStatus] idx:%d key:%s", idx, key);
        /**
        if (tmp == null) {
            //UFileUtils.Debug(cfg.getLogLevel(), "[removeUFileFileStatus] idx:%d  key:%s not hit", idx, key);
        } else {
            //UFileUtils.Debug(cfg.getLogLevel(), "[removeUFileFileStatus] idx:%d  key:%s hit", idx, key);
        }*/
        return hsUFS[idx].remove(key);
    }

    public UFileFileStatus getUFileFileStatus(Configure cfg, String key) {
        int idx = hash(key) % hsUFS.length;
        return hsUFS[idx].get(key);
    }

    public void removeDir(String dir) {
        for (int i = 0; i < hsUFS.length; i++ )
        {
            for (Map.Entry<String, UFileFileStatus> entry : hsUFS[i].entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(dir)) {
                    hsUFS[i].remove(key);
                }
            }
        }
    }
}
