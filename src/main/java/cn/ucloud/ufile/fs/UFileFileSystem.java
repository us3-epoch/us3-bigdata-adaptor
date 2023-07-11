package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.api.object.DeleteObjectApi;
import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.api.object.ObjectListWithDirFormatApi;
import cn.ucloud.ufile.api.object.multi.MultiUploadInfo;
import cn.ucloud.ufile.api.object.multi.MultiUploadPartState;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import cn.ucloud.ufile.bean.CommonPrefix;
import cn.ucloud.ufile.bean.MultiUploadResponse;
import cn.ucloud.ufile.bean.ObjectContentBean;
import cn.ucloud.ufile.bean.ObjectListWithDirFormatBean;
import cn.ucloud.ufile.bean.ObjectProfile;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;
import cn.ucloud.ufile.fs.common.VmdsAddressProvider;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static cn.ucloud.ufile.UfileConstants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;


public class UFileFileSystem extends org.apache.hadoop.fs.FileSystem {
    /** UFile文件系统的Scheme名称 */
    public static String SCHEME = "ufile";

    public static Map<String, String> defaultUserMeta = null;

    /** 文件系统持有解析后的插件配置 */
    private Configure cfg = new Configure();

    /** 当前工作的URI ??? */
    private URI uri;

    /** 当前工作的路径 ??? */
    private Path workDir;

    /** ??? */
    private String username;

    /** 操作的UFile存储桶 */
    private String bucket;

    /** SDK文件操作需要的URL配置，避免每次调用生成一个 */
    public ObjectConfig objcfg;

    /** SDK文件操作需要的URL配置，避免每次调用生成一个 */
    private ObjectConfig objcfgForIO;

    /** SDK文件操作需要的公私钥信息，避免每次调用生成一个 */
    private UfileObjectLocalAuthorization objauth;

    private String rootPath;
    private UFileFileStatus rootStatus;
    private VmdsAddressProvider provider;

    public String getUsername() { return username; }

    public String toString() {
        return String.format("\n1. %s:%s\n 2. %s:%s\n 3. %s:%s\n 4. %s:%s\n 5. %s:%s",
                "uri", uri.toString(),
                "workDir", workDir.toString(),
                "username", username,
                "bucket", bucket,
                "rootDir", rootPath
        );
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        conf.setInt(IO_FILE_BUFFER_SIZE_KEY, Constants.IO_FILE_BUFFER_SIZE_DEFAULT);
        setConf(conf);
        /** Username is the current user at the time the FS was instantiated. */
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        try {
            uri = name;
            workDir = new Path(uri.toString());
            bucket = name.getAuthority();
            rootPath = String.format("%s://%s/",SCHEME, bucket);
            rootStatus = new UFileFileStatus(-1, true, 1, 1, -1, new Path(rootPath));

            cfg.Parse(conf);
            UFileUtils.Debug(cfg.getLogLevel(),"[initialize] cfg content:%s", cfg);
            objauth = new UfileObjectLocalAuthorization(cfg.getPublicKey(), cfg.getPrivateKey());
            if (cfg.isUseMDS()) {
                provider = new VmdsAddressProvider(cfg,vmdsAddress->{
                    ObjectConfig newObjcfg = new ObjectConfig("http://"+ vmdsAddress);
                    this.objcfg = newObjcfg;
                    this.cfg.setMDSHost("http://"+vmdsAddress);
                });
                try {
                    provider.startWatching();
                } catch (Exception e) {
                    throw new IOException("cannot watch zookeeper node");
                }
                if (cfg.getMDSHost().equals(bucket+"."+cfg.getEndPoint())){ throw new IOException("The metadata address format cannot be <bucket name>."+ cfg.getEndPoint()); }
            } else {
                objcfg = new ObjectConfig("http://" + bucket+"."+cfg.getEndPoint());
            }
            objcfgForIO = new ObjectConfig("http://" + bucket+"."+cfg.getEndPoint());
            /** 给SDK库安装拦截器 */
            UFileInterceptor.Install(cfg.getLogLevel(), cfg.getIoTimeout(), cfg.getRetryTimes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getScheme() { return UFileFileSystem.SCHEME; }

    @Override
    public URI getUri() { return uri; }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, bufferSize:%d", f, bufferSize);

        // metadata service have no support
        UFileFileStatus ufs = innerGetFileStatus(f);
        if (ufs == null) {
            throw new FileNotFoundException("Can't open " + f
                    + " because it is nonexistent");
        }

        if (ufs.isDirectory()) {
            throw new FileNotFoundException("Can't open " + f
                    + " because it is a directory");
        }

        OSMeta osMeta;
        try {
            osMeta = UFileUtils.ParserPath(uri, workDir, f);
        } catch (Exception e) {
            throw new IOException(String.format("open parser path:%s ", f.toString()), e);
        }

        if (ufs.isArchive()) {
            UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, but is archive type", f);
            int blockWaitTimes = 0;
            while (true) {
                if (ufs == null) {
                    throw new IOException(String.format("[open] file:%s's status is null under unfreezing!!", osMeta.getKey()));
                }
               UFileFileStatus.RestoreStatus rs = ufs.restoreStatus();
               ObjectRestoreExpiration ore = ufs.getORE();
               if (ore != null) {
                   UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, onGoing:%b expiration:%d", osMeta.getKey(), ore.onGoing, ore.expiration);
               }

               /** 已经激活直接读取该文件 */
               if (0 == rs.compareTo(UFileFileStatus.RestoreStatus.RESTOED)) {
                   UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, had resotred", f.toString());
                   break;
               }

                /** 正在解冻中，先睡眠，后重新获取文件状态信息 */
               if (0 == rs.compareTo(UFileFileStatus.RestoreStatus.UNFREEZING)) {
                   UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, on unfreezing...", f.toString());
                   ufs.blockTimeForUnFreezing(blockWaitTimes++);
                   Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
                   ufs = innerGetFileStatus(f);
                   continue;
               }

               if (0 == rs.compareTo(UFileFileStatus.RestoreStatus.UNRESTORE)) {
                   UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, need restored", f.toString());
                   innerRestore(osMeta);
                   Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
                   ufs = innerGetFileStatus(f);
                   continue;
               }

               if (0 == rs.compareTo(UFileFileStatus.RestoreStatus.UNKNOWN)) {
                   UFileUtils.Info(cfg.getLogLevel(), "[open] path:%s, is unknow status, jump from check loop", f.toString());
                   break;
               }
            }
            UFileUtils.Debug(cfg.getLogLevel(), "[open] path:%s, archive type can be readrestored, untile %d", f, ufs.getORE().expiration);
        }

        return new FSDataInputStream(
                new UFileInputStream(cfg,
                        objauth,
                        objcfgForIO,
                        bucket,
                        osMeta.getKey(),
                        ufs.getLen()));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[create] path:%s, permission:%s overwrite:%b bufferSize:%d, replication:%d blockSize:%d", f,
                    permission, overwrite, bufferSize, replication, blockSize);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);

        if (!cfg.isUseMDS()) { checkNeedMkParentDirs(f, permission); }

        if (cfg.isUseAsyncWIO()) {
            return new FSDataOutputStream(innerCreate(permission, osMeta, Constants.UPLOAD_DEFAULT_MIME_TYPE, blockSize), statistics);
        }
        return new FSDataOutputStream(innerCreate(permission, overwrite, bufferSize, replication, blockSize, progress, osMeta, true, objcfgForIO), statistics);
    }

    /**
     * 功能一: 普通文件写IO
     * 功能二: 创建目录文件
     * @return
     * @throws IOException
     */
    public UFileOutputStream innerCreate(FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress, OSMeta osMeta, Boolean needBuf, ObjectConfig  objCfg) throws IOException {
        Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
        return new UFileOutputStream(
                this,
                cfg,
                objauth,
                objCfg,
                osMeta,
                permission,
                overwrite,
                bufferSize,
                replication,
                blockSize,
                progress,
                needBuf);
    }

    /**
     * 功能一: 普通文件写IO
     * @return
     * @throws IOException
     */
    private UFileAsyncOutputStream innerCreate(FsPermission permission, OSMeta osMeta, String mimeType, long blockSize) throws IOException {
        Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
        return new UFileAsyncOutputStream(
              this,
              cfg,
              objauth,
              objcfgForIO,
              permission,
              osMeta,
              mimeType,
              blockSize);
    };

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[append] path:%s, bufferSize:%d, progress:%s", f, bufferSize, progress);
        throw new UnsupportedOperationException("Append is not supported " + "by UFileFileSystem");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[rename] src path:%s, dst path:%s", src, dst);
        return innerRename(src, dst);
    }

    private boolean innerRename(Path src, Path dst) throws IOException {
        OSMeta srcOsMeta = UFileUtils.ParserPath(uri, workDir, src);
        OSMeta dstOsMeta = UFileUtils.ParserPath(uri, workDir, dst);

        if (cfg.isUseMDS()) {
            return ufileRename(srcOsMeta.getKey(), dstOsMeta.getKey());
        }


        if (srcOsMeta.getKey().isEmpty()) {
            throw new IOException(String.format("%s -> %s, source is root directory", srcOsMeta.getKey(),
                    dstOsMeta.getKey()));
        }

        if (dstOsMeta.getKey().isEmpty()) {
            throw new IOException(String.format("%s -> %s, dest is root directory", srcOsMeta.getKey(),
                    dstOsMeta.getKey()));
        }

        if (srcOsMeta.getKey().equals(dstOsMeta.getKey())) {
            throw new IOException(String.format("%s -> %s, source same with dest ", srcOsMeta.getKey(),
                    dstOsMeta.getKey()));
        }

        UFileFileStatus srcFs = innerGetFileStatus(src);
        if (srcFs == null) {
            throw new FileNotFoundException(String.format("%s -> %s, source is not exists", srcOsMeta.getKey(),
                    dstOsMeta.getKey()));
        }

        Path srcParent =  src.getParent();
        Path dstParent =  dst.getParent();
        if (dstOsMeta.getKey().startsWith(srcOsMeta.getKey()) &&
            dstOsMeta.getKey().toCharArray()[srcOsMeta.getKey().length()] == '/') {
            throw new IOException(String.format("%s -> %s, cannot rename a directory to a subdirectory of itsel", srcOsMeta.getKey(),
                    dstOsMeta.getKey()));
        }

        Constants.ufileMetaStore.removeUFileFileStatus(dstOsMeta.getKey());
        UFileFileStatus dstFs = innerGetFileStatus(dst);
        if (dstFs == null ) {
            /** 父目录必须存在 */
            UFileUtils.Debug(cfg.getLogLevel(),"[innerRename] dst:%s not exist", dst);
            OSMeta dstParentOsMeta = UFileUtils.ParserPath(uri, workDir, dstParent);
            if (!dstParentOsMeta.getKey().isEmpty()) {
                /** 如果目的端的父目录不存在 or 父目录不是目录都认为异常 */
                UFileUtils.Debug(cfg.getLogLevel(),"[innerRename] dst:%s check parent:%s exist", dst, dstParentOsMeta.getKey());
                FileStatus dstPFs = innerGetFileStatus(dst.getParent());
                if (dstPFs == null) {
                    throw new IOException(String.format("%s -> %s, dest has no parent", srcOsMeta.getKey(),
                            dstOsMeta.getKey()));
                }

                if (!dstPFs.isDirectory()) {
                    throw new IOException(String.format("%s -> %s, dest is not a directory", srcOsMeta.getKey(),
                            dstOsMeta.getKey()));
                }
                UFileUtils.Debug(cfg.getLogLevel(),"[innerRename] dst:%s parent:%s exist", dst, dstParentOsMeta.getKey());
            }
        } else {
            UFileUtils.Debug(cfg.getLogLevel(),"[innerRename] dst:%s exist", dst);
            if (srcFs.isDirectory()) {
                if (dstFs.isFile()) {
                    /** 不能把目录重命名为已有的文件 */
                    throw new IOException(String.format("%s -> %s, source is directory, but dst is file", srcOsMeta.getKey(),
                            dstOsMeta.getKey()));
                } else {
                    /** 如果目录不为空目录, 则拒绝覆盖 */
                    /** HDFS的行为是在该目录下创建一个源目录，并把所有文件拷贝到新目录下，所以后需要做支持 */
                    // TODO 判断目录是否为空目录
                    throw new IOException(String.format("%s -> %s, source is directory, but dst is not empty directory", srcOsMeta.getKey(),
                            dstOsMeta.getKey()));
                }
            } else {
                if (dstFs.isFile()) {
                    //throw new FileAlreadyExistsException(String.format("%s -> %s, dst is existing file", srcOsMeta.getKey(),
                    //        dstOsMeta.getKey()));
                    return false;
                }
            }
        }

        if (srcFs.isFile()) {
            /** 从文件拷贝到目录, 经过前面的拦截判断, 目的端只可能是根目录、空目录或者是不存在的目录 */
            if (dstFs != null && dstFs.isDirectory()) {
                    /** 文件到目录下 */
                    String newDstKey = dstOsMeta.getKey();
                    if (!newDstKey.endsWith("/")) {
                        newDstKey += "/";
                    }
                    newDstKey += src.getName();
                    UFileUtils.Debug( cfg.getLogLevel(), "[innerRename] src:%s is file, dst:%s is dir, newDst:%s ",
                            srcOsMeta.getKey(), dstOsMeta.getKey(), newDstKey);
                    ufileRename(srcOsMeta.getKey(), newDstKey);
                    UFileUtils.KeepListFileExistConsistency(this, newDstKey, Constants.DEFAULT_MAX_TRYTIMES*5, true);
                    UFileUtils.KeepListFileExistConsistency(this, srcOsMeta.getKey(), Constants.DEFAULT_MAX_TRYTIMES*5, false);
            } else if (dstFs == null){
                /** 文件到文件 */
                FsPermission perm = UFileFileStatus.parsePermission(UFileFileSystem.getDefaultUserMeta());
                ufileRename(srcOsMeta.getKey(), dstOsMeta.getKey());
                UFileUtils.KeepListFileExistConsistency(this, dstOsMeta.getKey(), Constants.DEFAULT_MAX_TRYTIMES*5, true);
                UFileUtils.KeepListFileExistConsistency(this, srcOsMeta.getKey(), Constants.DEFAULT_MAX_TRYTIMES*5,false );
            } else {
                //throw new FileAlreadyExistsException(dstFs.getPath().toString() + " exist!!");
                return false;
            }
        } else {
            /** 目录到目录 */
            if (dstFs != null && dstFs.isFile()) {
                throw new IOException(String.format("%s is directory", srcFs.getPath().toString()));
            }

            /** 从目录到目录的拷贝 */
            String dstKey = dstOsMeta.getKey();
            String srcKey = srcOsMeta.getKey();
            OSMeta osSrcParentMeta = UFileUtils.ParserPath(uri, workDir, srcParent);
            String srcParentKey = osSrcParentMeta.getKey();

            if (!dstKey.endsWith("/")) dstKey += "/";

            if (!srcParentKey.endsWith("/")) srcParentKey += "/";

            if (!srcKey.endsWith("/")) srcKey += "/";

            if (srcParentKey.equals(dstKey)) {
                throw new PathIOException(String.format("[innerRename] dir src:%s has under dir dst:%s ",
                        srcKey,
                        dstKey));
            }

            UFileUtils.Debug( cfg.getLogLevel(), "[innerRename] srcKey dir:%s to dstKey dir:%s ",
                    srcKey,
                    dstKey);

            String nextMarker = "";
            Map<String, String> delayRename = new HashedMap();
            do{
                ListUFileResult listRs = listUFileStatus(srcKey, nextMarker);
                if (listRs.fss != null) {
                    nextMarker = listRs.nextMarker;
                }

                if (listRs.fss == null) continue;
                String newDstKey = null;
                for (UFileFileStatus fs : listRs.fss) {
                    OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, fs.getPath());
                    String key = osMeta.getKey();
                    if (dstFs == null) {
                        /** /A/B/C => /X/Y/Z , Z not exist, result: /X/Y/Z **/
                        if (srcKey.length() == key.length()+1) {
                            if ((srcKey.lastIndexOf('/')+1) == srcKey.length()) {
                                // srcKey is '/A/B/C/', key is '/A/B/C', deal with by follow logic
                                continue;
                            } else {
                                throw new IOException(String.format("[innerRename] srcKey:%s => dstKey:%s, but build key is %s", srcKey, dstKey, key));
                            }
                        } else {
                            newDstKey = dstKey + key.substring(srcKey.length());
                        }
                        UFileUtils.Debug(cfg.getLogLevel(), "[innerRename] dst: %s not exist, key :%s is to newDstKey:%s ", dstKey, key, newDstKey);
                    } else {
                        /** /A/B/C => /X/Y/Z , Z exist, result: /X/Y/Z/C **/
                        newDstKey = dstKey + key.substring(srcParentKey.length());
                        UFileUtils.Debug(cfg.getLogLevel(), "[innerRename] dst: %s exist, key :%s is to newDstKey:%s ", dstKey, key, newDstKey);
                    }
                    /** 存着后面做rename操作 */
                    delayRename.put(key, newDstKey);
                    /** 如果是目录，还需操作以"/"结尾的 */
                    if (fs.isDirectory()) delayRename.put(key+"/", newDstKey+"/");
                }
            } while(!nextMarker.equals(""));

            srcKey = srcOsMeta.getKey();
            dstKey = dstOsMeta.getKey();
            if (dstFs == null) {
                /** /A/B/C => /X/Y/Z , Z not exist, result: /X/Y/Z **/
                delayRename.put(srcKey, dstKey);
                delayRename.put(srcKey+"/", dstKey+"/");
            } else {
                /** /A/B/C => /X/Y/Z , Z exist, result: /X/Y/Z/C **/
                String newDstKey = dstKey + "/" + srcKey.substring(srcParentKey.length());
                delayRename.put(srcKey, newDstKey);
                delayRename.put(srcKey+"/", newDstKey+"/");
            }

            String last = null;
            if (delayRename != null) {
                Iterator<Map.Entry<String, String>> it = delayRename.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = it.next();
                    /** TODO 线程池并发优化 */
                    UFileUtils.Debug(cfg.getLogLevel(), "[innerRename] delayRename key:%s to value:%s ",
                            entry.getKey(), entry.getValue());
                    try {
                        ufileRename(entry.getKey(), entry.getValue());
                        last = entry.getValue();
                    } catch (FileNotFoundException e) {
                        /** 证明该文件应该是以"/"结尾的目录，这个是新版插件创建目录时，创建的一个文件，但老插件没有，需要兼容*/
                        if (!entry.getKey().endsWith("/")) {
                            UFileUtils.Info(cfg.getLogLevel(),"[innerRename] suspect key:%s => value:%s is dir end with \"/\"", entry.getKey(), entry.getValue());
                            continue;
                        }

                        /** 创建以"/"结尾的目录 */
                        UFileFileStatus ufs = innerGetFileStatus(new Path(rootPath, entry.getValue().substring(entry.getValue().length()-1)));
                        if (ufs == null) {
                            ufs = innerGetFileStatus(new Path(rootPath, entry.getKey().substring(entry.getKey().length()-1)));
                        }

                        FsPermission perm = null;
                        if (ufs != null) {
                            if (!ufs.isDirectory()) {
                                throw  UFileUtils.TranslateException("[innerRename] suspect key:\"%s\\\" is dir end with \"/\", but status is not dir", entry.getKey(), e);
                            }
                            perm = ufs.getPermission();
                        } else {
                            perm = UFileFileStatus.parsePermission(UFileFileSystem.getDefaultUserMeta());
                        }
                        innerMkdir(new Path(rootPath, entry.getValue()), perm);
                    }
                }

                UFileUtils.KeepListDirExpectConsistency(this, srcKey, Constants.DEFAULT_MAX_TRYTIMES*5, 0);
                if (last != null) {
                    // 只检查最后一个, 因为有可能中途中断，为了保证幂等
                    UFileUtils.KeepListFileExistConsistency(this, last, Constants.DEFAULT_MAX_TRYTIMES*5, true);
                }
            }
        }
        return true;
    }

    /**
     * invoke UFile rename API, but hadoop plugin just invoke under same bucket.
     * @param srcKey
     * @param dstKey
     * @return
     * @throws IOException
     */
    private boolean ufileRename(String srcKey, String dstKey) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[ufileRename] srcKey:%s exist, dstKey:%s ", srcKey, dstKey);
        OSMeta srcMeta = new OSMeta(bucket, srcKey);
        OSMeta dstMeta = new OSMeta(bucket, dstKey);

        try {
            innerCopyFile(srcMeta, dstMeta, null, null);
            DeleteObjectApi request = UfileClient.object(objauth, objcfg).deleteObject(srcKey, bucket);
            /*有可能发给mds，删除目录时会耗时长*/
            request.setReadTimeOut(300*1000);
            request.execute();
            return true;
        } catch (UfileClientException e) {
            UFileUtils.Error(cfg.getLogLevel(), "[ufileRename] client, srcKey:%s exist, dstKey:%s, %s ",
                    srcKey, dstKey, e.toString());
            throw UFileUtils.TranslateException(String.format("ufileRename to %s", dstKey), srcKey, e);
        } catch (UfileServerException e) {
            UFileUtils.Error(cfg.getLogLevel(), "[ufileRename] server, srcKey:%s exist, dstKey:%s, %s ",
                    srcKey, dstKey, e.toString());
            throw UFileUtils.TranslateException(String.format("ufileRename to %s", dstKey), srcKey, e);
        }
    }

    /**
     * list ufile objects without use delimeter "/" for rename && delete
     * @param prefix
     * @param nextMark
     * @return
     * @throws IOException
     */
    private ListUFileResult listUFileStatus(String prefix, String nextMark) throws IOException {
        ListUFileResult lurs = new ListUFileResult();
        List<UFileFileStatus> result = new LinkedList<>();
        Path f = new Path(prefix);
        int idx = 0;
        HashMap hm = new HashMap<>();
        int retryCount = 1;
        while(true){
        try {
             ObjectListWithDirFormatBean response =UfileClient.object(objauth, objcfg)
                     .objectListWithDirFormat(bucket)
                     .withPrefix(prefix)
                     .withMarker(nextMark)
                     .dataLimit(Constants.LIST_OBJECTS_DEFAULT_LIMIT).execute();

             List<CommonPrefix> dirs = response.getCommonPrefixes();
             if (dirs != null) {
                 for (int i = 0; i < dirs.size(); i++) {
                     CommonPrefix cp = dirs.get(i);
                     hm.put(cp.getPrefix(), idx);
                     idx++;
                     result.add(new UFileFileStatus(0,
                         true,
                         1,
                         0,
                         0,
                         new Path(rootPath, dirs.get(i).getPrefix())));
                 }
             }

             List<ObjectContentBean> objs = response.getObjectContents();
             if (objs != null) {
                 for (int i = 0; i < objs.size(); i++) {
                     ObjectContentBean obj = objs.get(i);
                     String key = obj.getKey();

                     if (obj.getMimeType().equals(Constants.DIRECTORY_MIME_TYPE_1)) {
                         key += "/";
                     } else if (!obj.getMimeType().equals(Constants.DIRECTORY_MIME_TYPE_2)) {
                         result.add(new UFileFileStatus(
                                 Long.parseLong(obj.getSize()),
                                 false,
                                 1,
                                 Constants.DEFAULT_HDFS_BLOCK_SIZE,
                                 /** 反正该接口不会关注时间 */
                                 0,
                                 0,
                                 //obj.getLastModified()*1000,
                                 //obj.getLastModified()*1000,
                                 null,
                                 null,
                                 null,
                                 new Path(rootPath, key)));
                         continue;
                     }

                     /** 证明这是一个目录 */
                     if (hm.containsKey(key)) {
                         int index = (int)(hm.get(prefix));
                         result.set(index, new UFileFileStatus(this,
                                 0,
                                 true,
                                 /** 反正该接口不会关注时间 */
                                 0,
                                 0,
                                 //obj.getLastModified()*1000,
                                 //obj.getLastModified()*1000,
                                 new Path(rootPath, key),
                                 null));
                     } else {
                         result.add(new UFileFileStatus(0,
                                 true,
                                 1,
                                 0,
                                 0,
                                 new Path(rootPath, key)));
                     }
                }
            }

            lurs.nextMarker = response.getNextMarker();
            UFileUtils.Debug(cfg.getLogLevel(),"[listUFileStatus] prefix:%s nextMark:%s rspNextMark:%s limit:%d",
                    prefix,
                    nextMark,
                    lurs.nextMarker,
                    Constants.LIST_OBJECTS_DEFAULT_LIMIT);

            if (result.size() != 0) lurs.fss =  result;
            return lurs;
        } catch (UfileClientException e) {
            e.printStackTrace();
            if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException("[listUFileStatus] exception too much times, ", f.toString(), e);
        } catch (UfileServerException e) {
            e.printStackTrace();
            if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES||e.getErrorBean().getResponseCode()<500)
            throw UFileUtils.TranslateException("[listUFileStatus] exception too much times, ", f.toString(), e);
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

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[delete] path:%s, recursive:%b", f, recursive);
        try {
            UFileFileStatus ufs = innerGetFileStatus(f);
            /** 在distcp场景下如果抛出异常，会失败，所以返回TRUE */
            //if (ufs == null) throw new FileNotFoundException(String.format("%s is not exist", f.toString()));
            if (ufs == null) return true;
            return innerDelete(f, recursive, ufs);
        } catch (UfileClientException e) {
            throw UFileUtils.TranslateException("[delete] delete failed", f.toString(), e);
        }
    }

    private boolean innerDelete(Path f, boolean recursive, UFileFileStatus ufs) throws IOException, UfileClientException {
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);
        if (!cfg.isUseMDS()) {
            String key = osMeta.getKey();
            if (ufs == null) {
                throw new FileNotFoundException(String.format("[innerDelete] %s not found", key));
            }

            if (ufs.isDirectory()) {
                UFileUtils.Debug(cfg.getLogLevel(), "[innerDelete] f:%s is dir recursive:%b", f, recursive);
                if (!key.endsWith("/")) {
                    key += "/";
                }

                if (key.equals("/")) {
                    throw new IOException("Cannot delete root path");
                }

                /** 获取当前目录下的文件或者目录，如果recursive为TRUE，则对目录进行递归删除*/
                // TODO 优化点,应该考虑到目录下文件很多的情况,拉一批删一批的情况,这样避免内存消耗.
                UFileFileStatus[] fss = innerListStatus(f, ufs);
                for (UFileFileStatus ufileFS: fss) {
                    if (ufileFS.isDirectory() && recursive) {
                            UFileUtils.Debug(cfg.getLogLevel(), "[innerDelete] f:%s is sub dir", ufileFS.getPath());
                            innerDelete(ufileFS.getPath(), recursive, ufileFS);
                    } else {
                        try {
                            ufileDeleteFile(ufileFS);
                        } catch (FileNotFoundException e) {
                            UFileUtils.Error(cfg.getLogLevel(), "[innerDelete] file not found, %s ", e.toString());
                        }
                    }
                }

                /** 如果不是递归删除，那么这个目录就不能删除掉*/
                if (!recursive) return true;
                if (fss != null && fss.length > 0) {
                    UFileUtils.KeepListDirExpectConsistency(this, osMeta.getKey(), Constants.DEFAULT_MAX_TRYTIMES*10, 0);
                }
            }
        }

        /** 清理了文件后删除该文件或者目录 */
        try {
            boolean result = ufileDeleteFile(ufs);
            if (!cfg.isUseMDS()) UFileUtils.KeepListFileExistConsistency(this, osMeta.getKey(), Constants.DEFAULT_MAX_TRYTIMES*5, false);
            return result;
        } catch (FileNotFoundException e) {
            UFileUtils.Error(cfg.getLogLevel(), "[innerDelete] last delete file not found, %s ", e.toString());
            return true;
        }
    }

    private boolean ufileDeleteFile(UFileFileStatus ufs) throws IOException {
        Path f = ufs.getPath();
        UFileUtils.Debug(cfg.getLogLevel(), "[ufileDeleteFile] delete %s ", f);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);
        String key = osMeta.getKey();
        int retryCount = 1;
        while(true){
        try {
            Constants.ufileMetaStore.removeUFileFileStatus(key);
            DeleteObjectApi request = UfileClient.object(objauth, objcfg).deleteObject(key, bucket);
            /*有可能发给mds，删除目录时会耗时长*/
            request.setReadTimeOut(300*1000);
            request.execute();
        } catch (UfileClientException e) {
            UFileUtils.Error(cfg.getLogLevel(),"[ufileDeleteFile] %s client, %s ", key, e.toString());
            if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES)
            return false;
            continue;
        } catch (UfileServerException e) {
            if (e.getErrorBean().getResponseCode() == Constants.API_NOT_FOUND_HTTP_STATUS) {
                UFileUtils.Info(cfg.getLogLevel(),"[ufileDeleteFile] %s server, %s ", key, e.toString());
            } else {
                UFileUtils.Error(cfg.getLogLevel(),"[ufileDeleteFile] %s server, %s ", key, e.toString());
            }
            if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES||e.getErrorBean().getResponseCode()<500)
            continue;
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }

        if (ufs.isDirectory() && !cfg.isUseMDS()) {
            try {
                if (!key.endsWith("/")) key += "/";
                else key = key.substring(0, key.length()-1);
                Constants.ufileMetaStore.removeUFileFileStatus(key);
                UfileClient.object(objauth, objcfg).deleteObject(key, bucket).execute();
                Constants.ufileMetaStore.removeDir(key);
                return true;
            } catch (UfileClientException e) {
                UFileUtils.Error(cfg.getLogLevel(),"[ufileDeleteFile] %s client, %s ", key, e.toString());
                return false;
            } catch (UfileServerException e) {
                if (e.getErrorBean().getResponseCode() == Constants.API_NOT_FOUND_HTTP_STATUS) {
                    UFileUtils.Info(cfg.getLogLevel(), "[ufileDeleteFile] %s server, %s ", key, e.toString());
                    return true;
                } else {
                    UFileUtils.Error(cfg.getLogLevel(),"[ufileDeleteFile] %s server, %s ", key, e.toString());
                    return false;
                }
            }
        }
        return true;
    }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[listStatus] path:%s ", f);
        // 保持跟HDFS语义一致
        UFileFileStatus ufs = innerGetFileStatus(f);
        if (ufs == null) throw new FileNotFoundException(String.format("%s not exist!!", f));
        if (ufs.isFile()) {
            UFileFileStatus[] stats = new UFileFileStatus[1];
            stats[0]= ufs;
            return stats;
        }
        return innerListStatus(f, ufs);
    }

    private UFileFileStatus[] innerListStatus(Path f, UFileFileStatus fileStatus) throws IOException{
        return innerListStatusWithSize(f, fileStatus, false, Constants.LIST_OBJECTS_DEFAULT_LIMIT, false);
    }

    public UFileFileStatus[] innerListStatusWithSize(Path f, UFileFileStatus fileStatus, boolean isOneLoop, int dataLimit, boolean forStatus) throws IOException {
        if (fileStatus.isDirectory()) {
          return genericInnerListStatusWithSize(f, true, isOneLoop, dataLimit, forStatus);
        } else {
            UFileFileStatus[] stats = new UFileFileStatus[1];
            stats[0]= fileStatus;
            return stats;
        }
    }

    public UFileFileStatus[] genericInnerListStatusWithSize(Path f, boolean isDir, boolean isOneLoop, int dataLimit, boolean forStatus) throws IOException {
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);
        String prefix = osMeta.getKey();
        return genericInnerListStatusWithSize(prefix, isDir, isOneLoop, dataLimit, forStatus);
    }

    public UFileFileStatus[] genericInnerListStatusWithSize(String prefix, boolean isDir, boolean isOneLoop, int dataLimit, boolean forStatus) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[innerListStatusWithSize] path:%s ", prefix);
        int idx = 0;
        if (isDir) {
            List<UFileFileStatus> result = new ArrayList<UFileFileStatus>();
            if (!prefix.isEmpty() && !forStatus) if (!prefix.endsWith("/")) prefix = prefix + '/';

            String nextMark = "";
            Exception exception = null;
            /** 用来临时存储目录信息，用来过滤文件中老插件不以"/"结尾的目录
             *  不过该方法有个条件保证，就是得目录先返回，不过确认后端服务是
             *  保证了该条件。
             */
            HashMap hm = new HashMap<>();
            int retryCount = 1;
            while (true) {
                try {
                    ObjectListWithDirFormatApi request = UfileClient.object(objauth, objcfg)
                            .objectListWithDirFormat(bucket)
                            .withPrefix(prefix)
                            .withMarker(nextMark)
                            .dataLimit(dataLimit);

                    if (!forStatus) {
                            request.withDelimiter(Constants.LIST_OBJECTS_DEFAULT_DELIMITER);
                    }

                    ObjectListWithDirFormatBean response = request.execute();
                    List<CommonPrefix> dirs = response.getCommonPrefixes();
                    if (dirs != null) {
                        for (int i = 0; i < dirs.size(); i++) {
                            CommonPrefix cp = dirs.get(i);
                            hm.put(cp.getPrefix(), idx);
                            idx++;
                            UFileUtils.Debug(cfg.getLogLevel(), "[innerListStatus][%d] common prefix:%s ", i, dirs.get(i).getPrefix());
                            result.add(new UFileFileStatus(0,
                                    true,
                                    1,
                                    0,
                                    0,
                                    0,
                                    null,
                                    username,
                                    Constants.superGroup,
                                    new Path(rootPath, cp.getPrefix())));
                        }
                    }

                    List<ObjectContentBean> objs = response.getObjectContents();
                    if (objs != null) {
                        for (int i = 0; i < objs.size(); i++) {
                            UFileUtils.Debug(cfg.getLogLevel(), "[innerListStatus][%d] key:%s lastModified:%d ", i, objs.get(i).getKey(), objs.get(i).getLastModified());
                            ObjectContentBean obj = objs.get(i);
                            Map<String, String> userMeta = obj.getUserMeta();
                            /** TODO 测试好后进行清理 */
                            //if (userMeta != null &&
                            //        cfg.getLogLevel().ordinal() <= LOGLEVEL.DEBUG.ordinal()) {
                            //    Iterator it = userMeta.entrySet().iterator();
                            //    while (it.hasNext()) {
                            //        Map.Entry item = (Map.Entry) it.next();
                            //        UFileUtils.Debug(cfg.getLogLevel(), "[innerListStatus] userMeta key:%s val:%s  ", (String) item.getKey(), (String) item.getValue());
                            //    }
                            //}
                            String key = obj.getKey();

                            if (obj.getMimeType().equals(Constants.DIRECTORY_MIME_TYPE_1)) {
                                key += "/";
                                /**
                                 *   证明这就是一个目录
                                 *   新插件会创建一个以"/"的目录和一个mimetype是"file/path"的目录
                                 *   是为了兼容GET时的问题
                                 */
                            } else if (!obj.getMimeType().equals(Constants.DIRECTORY_MIME_TYPE_2)) {
                                result.add(new UFileFileStatus(this,
                                        Long.parseLong(obj.getSize()),
                                        false,
                                        obj.getLastModified() * 1000,
                                        obj.getLastModified() * 1000,
                                        new Path(rootPath, key),
                                        userMeta));
                                continue;
                            }

                            /** 证明这是一个目录 */
                            if (hm.containsKey(key)) {
                                int index = (int) (hm.get(key));
                                result.set(index, new UFileFileStatus(this,
                                        0,
                                        true,
                                        obj.getLastModified() * 1000,
                                        obj.getLastModified() * 1000,
                                        new Path(rootPath, key),
                                        userMeta));
                            } else {
                                result.add(new UFileFileStatus(this,
                                        0,
                                        true,
                                        obj.getLastModified() * 1000,
                                        obj.getLastModified() * 1000,
                                        new Path(rootPath, key),
                                        userMeta));
                            }
                        }
                    }

                    nextMark = response.getNextMarker();
                    if (nextMark.equals("") || isOneLoop ) {
                        UFileUtils.Debug(cfg.getLogLevel(), "[innerListStatus] encounters the end");
                        return result.toArray(new UFileFileStatus[result.size()]);
                    }
                    continue;
                } catch (UfileClientException e) {
                    retryCount++;
                    if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES){
                    exception = e;
                    UFileUtils.Error(cfg.getLogLevel(),"[innerListStatus] client, %s ", e.toString());
                    throw UFileUtils.TranslateException("[innerListStatus]", prefix, exception);
                    }
                } catch (UfileServerException e) {
                    retryCount++;
                    if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES||e.getErrorBean().getResponseCode()<500){
                    exception = e;
                    if (e.getErrorBean().getResponseCode()/100 == 4) {
                        throw UFileUtils.TranslateException("[innerListStatus]", prefix, exception);
                    }
                    UFileUtils.Error(cfg.getLogLevel(),"[innerListStatus] server, %s ", e.toString());
                    throw UFileUtils.TranslateException("[innerListStatus]", prefix, exception);
                }
                }
            } // end of while statement
        }
        return UFileFileStatus.ufs;
    }

    @Override
    public synchronized void setWorkingDirectory(Path new_dir) {
        UFileUtils.Debug(cfg.getLogLevel(), "[setWorkingDirectory] path:%s ", new_dir);
        workDir = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
        UFileUtils.Debug(cfg.getLogLevel(), "[getWorkingDirectory] working directory:%s", workDir);
        return workDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[mkdirs] path:%s, FsPermission:%s", f, permission);
        UFileFileStatus ufs;
        ufs = innerGetFileStatus(f);
        if (ufs != null) {
            if (ufs.isDirectory()) return true;
            else new FileAlreadyExistsException("Path is a file:" + f);
        }

        if (!cfg.isUseMDS()) { checkNeedMkParentDirs(f, permission); }

        return innerMkdir(f, permission);
    }

    private boolean innerMkdir(Path f, FsPermission permission) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[innerMkdir] path:%s, FsPermission:%s", f, permission);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);

        //this.getFileStatus(f.getParent());

        if (cfg.isUseMDS()) {
            UFileOutputStream stream =innerCreate(permission, false, 0, (short) 0,0 , null, osMeta, false, objcfg);
            stream.setMimeType(Constants.DIRECTORY_MIME_TYPE_2);
            stream.close();
        } else {
            if (osMeta.getKey().endsWith("/")) {
                UFileOutputStream stream =innerCreate(permission, false, 0, (short) 0,0 , null, osMeta, false, objcfg);
                stream.setMimeType(Constants.DIRECTORY_MIME_TYPE_2);
                stream.close();

                osMeta.setKey(osMeta.getKey().substring(0, osMeta.getKey().length()-1));
                stream =innerCreate(permission, false, 0, (short) 0,0 , null, osMeta, false, objcfg);
                stream.setMimeType(Constants.DIRECTORY_MIME_TYPE_1);
                stream.close();
            } else {
                UFileOutputStream stream =innerCreate(permission, false, 0, (short) 0,0 , null, osMeta, false, objcfg);
                stream.setMimeType(Constants.DIRECTORY_MIME_TYPE_1);
                stream.close();

                osMeta.setKey(osMeta.getKey()+"/");
                stream =innerCreate(permission, false, 0, (short) 0,0 , null, osMeta, false, objcfg);
                stream.setMimeType(Constants.DIRECTORY_MIME_TYPE_2);
                stream.close();
            }
        }
        return true;
    }

    private void checkNeedMkParentDirs(Path f, FsPermission permission) throws FileAlreadyExistsException, IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[checkNeedMkParentDir] path:%s", f);
        UFileFileStatus ufs;
        while (true) {
            f = f.getParent();
            UFileUtils.Debug(cfg.getLogLevel(), "[checkNeedMkParentDir] parent path:%s", f);
            ufs = innerGetFileStatus(f);
            /** 不存在则创建 */
            if (ufs == null) innerMkdir(f, permission);
            /** 只要到上级目录碰到存在就结束，因为root目录是虚拟出来的，所以会最多到根目录结束 */
            else if (ufs.isDirectory()) break;
            else if (ufs.isFile()) throw new FileAlreadyExistsException(String.format("need mkdir ,but %s is file", f));
        }
    }

    /**
     * 返回一个代表Path的FileStatus对象
     * @param f
     * @return 一个FileStatus对象
     * @throws IOException
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[getFileStatus] path:%s", f);
        UFileFileStatus ufs = innerGetFileStatus(f);
        if (ufs == null) throw new FileNotFoundException(String.format("%s not exist!!", f));
        return ufs;
    }

    /**
     * 提供该函数主要是考虑到有些用户是直接把数据上传到UFile，然后大数据计算直接访问的，这一中间的目录可能
     * 缺失，需要靠拉取前缀下面是否有一个文件或者目录来判断该path是否为路径
     * @param f
     * @return
     * @throws IOException
     */
    private UFileFileStatus innerGetFileStatus(Path f) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[innerGetFileStatus] path:%s", f);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);
        UFileFileStatus ufs = innerSampleGetFileStatus(f, osMeta);
        if (!cfg.isUseMDS()) {
            if (ufs == null || (ufs != null && ufs.is404Cache())) {
                // 有可能是个目录
                UFileUtils.Debug(cfg.getLogLevel(), "[innerGetFileStatus] path:%s node exist, found %s", f, f);
                ufs = new UFileFileStatus(0, true,3, Constants.DEFAULT_HDFS_BLOCK_SIZE, (System.currentTimeMillis()/1000), f);
                UFileFileStatus[] ufArr = null;
                try {
                    // 可能返回两个，prefix和contents
                    ufArr = genericInnerListStatusWithSize(f, true, true, 1, true);
                } catch (FileNotFoundException e) {
                    return null;
                }
                /**
                 * 如果该前缀下面还有还有文件，则认为该文件为目录
                 */
                if (ufArr == null) return null;
                if (ufArr.length == 0) return null;
                boolean found = false;
                for (UFileFileStatus fs: ufArr) {
                    if (fs.getPath().equals(f)) {
                        // 找到一样文件名的文件
                        found = true;
                        ufs = fs;
                        break;
                    }
                }

                if (!found) { return null;}
                if (ufs.isFile()) {
                    // 这种情况下应该是业务先删除了这个普通文件，然后再来Head这个文件，由于列表服务一致性导致还可以拉到，但是
                    // 如果在同一个适配器内删除后，确保了该文件在列表服务也没有了话，则不会出现这种情况，不过如果是多个适
                    // 配器操作时，可能无法保证这一点，当然大数据分析模式主要是单个任务负责某个具体目录
                    throw new IOException(ufs.getPath().toString()  + " is regular file not directory!!");
                }

                if (!innerMkdir(f, ufs.getPermission())) {
                    UFileUtils.Error(cfg.getLogLevel(), "[innerGetFileStatus] mkdirs path:%s/ failure", f);
                    return null;
                }
                Constants.ufileMetaStore.putUFileFileStatus(cfg, osMeta.getKey(), ufs);
            }
        }
        return ufs;
    }

    /**
     * 返回一个代表path的文件状态对象
     * @param osMeta 是想要从中获取信息的路径
     * @return 一个FileStatus对象
     * @throws IOException
     */
    private UFileFileStatus innerSampleGetFileStatus(Path path, OSMeta osMeta) throws IOException {
        if (!osMeta.getBucket().equals(bucket)) {
            throw new IOException(String.format("path:%s bucket is ", osMeta.getKey().toString()) + osMeta.getBucket() + " but fs bucket is "+ bucket);
        }

        if (osMeta.getKey().length() == 0) {
            UFileUtils.Debug(cfg.getLogLevel(),"[innerGetFileStatus] this is root path");
            return rootStatus;
        }
        UFileFileStatus fs = null;
        /** 1. 如果没有使用mds则首先尝试从Cache中获取，否则直接发送请求去拿 */
        if(!cfg.isUseMDS()){
        fs = Constants.ufileMetaStore.getUFileFileStatus(cfg, osMeta.getKey());
        if (fs != null) {
            if (fs.is404Cache()) {
                UFileUtils.Debug(cfg.getLogLevel(), String.format("[innerGetFileStatus] %s is 404 cache, expired time:%d", osMeta.getKey(), fs.getCacheTimeout()));
                /** 404缓存 */
                if (!fs.timeOut404Cache()) {
                    /** 404没过期 */
                    UFileUtils.Debug(cfg.getLogLevel(), String.format("[innerGetFileStatus] %s is 404 cache, hit", osMeta.getKey()));
                    return null;
                }
                UFileUtils.Debug(cfg.getLogLevel(), String.format("[innerGetFileStatus] %s 404 cache is expired, time:%d", osMeta.getKey(), fs.getCacheTimeout()));
                Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
            } else if (!fs.timeOutCache()){
                return fs;
            }
            fs = null;
        }
    }
        /** 2. 调用SDK获取 */
        try {
            fs = ufileGetFileStatus(path, osMeta.getKey());
        } catch (FileNotFoundException e) {
            UFileUtils.Debug(cfg.getLogLevel(),"[innerGetFileStatus] file not found, %s", e.toString());
        } catch (IOException e) {
            UFileUtils.Error(cfg.getLogLevel(),"[innerGetFileStatus] io exception, %s", e.toString());
            throw UFileUtils.TranslateException("[innerGetFileStatus] io exception, ", path.toString(), e);
        }

        if (fs == null) {
            /** 添加404缓存 */
            UFileFileStatus cache = UFileFileStatus.Cache404();
            UFileUtils.Debug(cfg.getLogLevel(), String.format("[innerGetFileStatus] add (key:%s)'s 404 cache, expired time:%d", osMeta.getKey(), cache.getCacheTimeout()));
            Constants.ufileMetaStore.putUFileFileStatus(cfg, osMeta.getKey(),
                    cache);
            return null;
        }

        /** 3. 保存在Cache中 */
        Constants.ufileMetaStore.putUFileFileStatus(cfg, osMeta.getKey(), fs);
        return fs;
    }

    public UFileFileStatus ufileGetFileStatus(Path path, String key) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(), "[ufileGetFileStatus] query %s", key);
        if (!key.isEmpty()) {
            ObjectProfile objectProfile = null;
            Exception exception = null;
            int retryCount = 1;
            while(true){
            try {
                objectProfile = UfileClient.object(objauth, objcfg)
                        .objectProfile(key, bucket)
                        .execute();
                UFileUtils.Debug(cfg.getLogLevel(),"[ufileGetFileStatus] path:%s lastMod:%s  ",  key, objectProfile.getLastModified());
                long timeofAccMod = Constants.GMTDateTemplate.parse(objectProfile.getLastModified()).getTime();

                Map<String, String> userMeta = objectProfile.getMetadatas();
                /** TODO 测试好后进行清理 */
                //if (userMeta != null &&
                //    cfg.getLogLevel().ordinal() <= LOGLEVEL.DEBUG.ordinal()) {
                //    Iterator it = userMeta.entrySet().iterator();
                //    while(it.hasNext()) {
                //        Map.Entry item = (Map.Entry) it.next();
                //        UFileUtils.Debug(cfg.getLogLevel(),"[ufileGetFileStatus] userMeta key:%s val:%s  ",  (String)item.getKey(), (String)item.getValue());
                //    }
                //}

                UFileFileStatus ufs;
                /** 如果MIME类型为目录类型, 或者满足为目录的条件认为是目录 */
                if (Constants.DIRECTORY_MIME_TYPES.contains(objectProfile.getContentType().toLowerCase()) ||
                        UFileUtils.IsDirectory(key, objectProfile.getContentLength())
                ) {
                    ufs = new UFileFileStatus( this,
                            0,
                            true,
                            timeofAccMod,
                            timeofAccMod,
                            path,
                            userMeta);
                } else {
                    ufs = new UFileFileStatus( this,
                            objectProfile.getContentLength(),
                            false,
                            timeofAccMod,
                            timeofAccMod,
                            path,
                            userMeta) ;
                }

                /** 冷存激活相关信息提取 */
                if (UFileUtils.isArchive(objectProfile.getStorageType())) {
                    ObjectRestoreExpiration ore = UFileUtils.ParserRestore(objectProfile.getRestoreTime());
                    ufs.setORE(ore);
                }
                ufs.setStorageType(objectProfile.getStorageType());
                return ufs;
            }catch (UfileClientException e){
                System.out.println(objcfg.getCustomHost());
                retryCount++;
                if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES){
                    throw UFileUtils.TranslateException("[ufileGetFileStatus]", path.toString(), e);
                }
                try {
                    Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
                } catch (InterruptedException e1) {
                    throw new IOException("not able to handle exception", e1);
                }
            } catch(UfileServerException e){
                retryCount++;
                if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES||e.getErrorBean().getResponseCode()<500){
                    throw UFileUtils.TranslateException("[ufileGetFileStatus]", path.toString(), e);
                }
                try {
                    Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
                } catch (InterruptedException e1) {
                    throw new IOException("not able to handle exception", e1);
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
        return null;
    }

    /**
     * 关闭文件系统
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            if(provider != null){
                provider.stopWatching();
            }
            super.close();
        } finally {
            // TODO
        }
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        UFileFileStatus fs = innerGetFileStatus(f);
        if (fs == null) {
            UFileUtils.Info(cfg.getLogLevel(),"[getFileChecksum] f:%s is dir", f);
            throw new FileNotFoundException("[getFileChecksum] f:" + f.toString() + " no exist");
        } else if (fs.isDirectory()) {
            UFileUtils.Info(cfg.getLogLevel(),"[getFileChecksum] f:%s is dir", f);
            return null;
        }

        String hexCrc32c = fs.getHexCrc32c();
        if (hexCrc32c == null) {
            UFileUtils.Info(cfg.getLogLevel(),"[getFileChecksum] f:%s' crc32c is empty ", f.toString());
            return null;
        }

        UFileUtils.Debug(cfg.getLogLevel(),"[getFileChecksum] f:%s' crc32c is %s", f, hexCrc32c);
        return new UFileFileChecksum(hexCrc32c);
    }

    @Override
    public void setOwner(Path f, String username, String groupname
    ) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(),"[setOwner] f:%s' username:%s groupname:%s  ", f, username, groupname);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, f);
        UFileFileStatus ufs = Constants.ufileMetaStore.getUFileFileStatus(cfg, osMeta.getKey());
        if (ufs == null) {
            UFileUtils.Debug(cfg.getLogLevel(),"[setOwner] setXXX not set ufile status ");
            Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
            ufs = innerGetFileStatus(f);
            if (ufs == null) {
                UFileUtils.Info(cfg.getLogLevel(),"[setOwner] ufile status is empty after head again ");
                return;
            }
        }

        if (username == null || username.equals("")) username = ufs.getOwner();
        ufs.setOverrideUserName(username);

        if (groupname == null || groupname.equals("")) groupname = ufs.getGroup();
        ufs.setOverrideGroupName(groupname);

        Map<String, String> userMeta = extractUserMeta(username,groupname,ufs.getHexCrc32c(),ufs.getPermission(),
                ufs.getBlockSize(), ufs.getReplication(), ufs.getBase64Md5());

        replaceUserMeta(osMeta, ufs, userMeta);
    }

    @Override
    public void setPermission(Path p, FsPermission permission
    ) throws IOException {
        //UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] f:%s' permission:%s  ", p, permission);
        //UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] UserAction:%s  ", UFileUtils.EncodeFsAction(permission.getUserAction()));
        //UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] GroupAction:%s  ", UFileUtils.EncodeFsAction(permission.getGroupAction()));
        //UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] OtherAction:%s  ", UFileUtils.EncodeFsAction(permission.getOtherAction()));
        //UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] Sticy:%s  ", UFileUtils.EncodeFsSticky(permission.getStickyBit()));
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, p);
        UFileFileStatus ufs = Constants.ufileMetaStore.getUFileFileStatus(cfg, osMeta.getKey());
        Map<String, String> userMeta;
        if (ufs == null || ufs.is404Cache()) {
            UFileUtils.Debug(cfg.getLogLevel(),"[setPermission] setXXX not set ufile status ");
            Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
            ufs = innerGetFileStatus(p);
            if (ufs == null) {
                UFileUtils.Info(cfg.getLogLevel(),"[setPermission] ufile status is empty after head again ");
                return;
            }
            userMeta = extractUserMeta(ufs.getOwner(),
                    ufs.getGroup(),
                    ufs.getHexCrc32c(),
                    permission,
                    ufs.getBlockSize(),
                    ufs.getReplication(),
                    ufs.getBase64Md5());
        } else {
            String userName = ufs.getOverrideGroupName();
            String groupName = ufs.getOverrideGroupName();
            if (userName == null || userName.equals("")) userName = ufs.getOwner();
            if (groupName == null || groupName.equals("")) groupName = ufs.getGroup();
            userMeta = extractUserMeta(userName,
                    groupName,
                    ufs.getHexCrc32c(),
                    permission,
                    ufs.getBlockSize(),
                    ufs.getReplication(),
                    ufs.getBase64Md5());
        }

        replaceUserMeta(osMeta, ufs, userMeta);
    }

    private void innerCopyFile(OSMeta srcOsMeta, OSMeta dstOsMeta, String directive, Map<String, String> userMeta) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(),"[innerCopyFile] src:%s/%s dst:%s/%s directive:%s  ",
                srcOsMeta.getBucket(),
                srcOsMeta.getKey(),
                dstOsMeta.getBucket(),
                dstOsMeta.getKey(),
                directive);
        try {
            copyObject(srcOsMeta.getKey(), dstOsMeta.getKey());
        } catch (UfileServerException|UfileClientException e) {
            throw UFileUtils.TranslateException("copy object", srcOsMeta.getKey() + " -> " + dstOsMeta.getKey(),e);
        }
    }

    private void copyObject(String srcKey, String dstKey) throws UfileServerException, UfileClientException {
        UFileUtils.Debug(cfg.getLogLevel(),"[copyObject] srcKey:%s' dstKey:%s ", srcKey, dstKey);
        ObjectProfile srcObjProfile = getObjectProfile(srcKey);
        if (cfg.getMultiCopyPartThreshold() > srcObjProfile.getContentLength()) {
            UfileClient.object(objauth, objcfg)
                    .copyObject(bucket, srcKey)
                    .copyTo(bucket, dstKey)
                    .execute();
        } else {
            UFileUtils.Debug(cfg.getLogLevel(),"[copyObject] file size: %d", srcObjProfile.getContentLength());
            // 使用分片接口进行拷贝
            MultiUploadInfo uploadInfo = UfileClient.object(objauth, objcfg)
                    // 这里的 uploadTarget是 dstObject
                    .initMultiUpload(dstKey, srcObjProfile.getContentType(), srcObjProfile.getBucket())
                    .withStorageType(srcObjProfile.getStorageType())
                    .withMetaDatas(srcObjProfile.getMetadatas())
                    .execute();
            // 需要先转成double 否则会丢失精度
            int chunkCount = (int) Math.ceil((double) srcObjProfile.getContentLength() / MULTIPART_SIZE);
            // 优化：使用线程池进行同步上传
            List<MultiUploadPartState> partStateList = new ArrayList<>();
            try {
                for (int i = 0; i < chunkCount; i++) {
                    int start = i * MULTIPART_SIZE;
                    int end = start + MULTIPART_SIZE - 1;
                    if (end >= srcObjProfile.getContentLength()) {
                        end = (int) srcObjProfile.getContentLength();
                    }
                    MultiUploadPartState partState = UfileClient.object(objauth, objcfg)
                            .multiUploadCopyPart(uploadInfo, i, srcObjProfile.getBucket(), srcObjProfile.getKeyName(),
                                    start, end)
                            .execute();
                    partStateList.add(partState);
                    UFileUtils.Debug(cfg.getLogLevel(),"[copyObject] part state: %s, start: %d, end: %d", partState.toString(), start, end);
                }
                MultiUploadResponse res = UfileClient.object(objauth, objcfg)
                        .finishMultiUpload(uploadInfo, partStateList)
                        .execute();
                if (res.getFileSize() < srcObjProfile.getContentLength()) {
                    // 接口返回的fileSize 小于 之前查询到的源文件大小
                    UFileUtils.Error(cfg.getLogLevel(),"[copyObject] dst file size: %d less than src file size: %d",
                            res.getFileSize(), srcObjProfile.getContentLength());
                }
            } catch (UfileServerException | UfileClientException e) {
                // copy过程失败后要进行abort
                UfileClient.object(objauth, objcfg)
                        .abortMultiUpload(uploadInfo)
                        .execute();
                throw e;
            }
        }
    }

    protected ObjectProfile getObjectProfile(String key) throws UfileServerException, UfileClientException {
        return UfileClient.object(objauth, objcfg)
                .objectProfile(key, bucket)
                .execute();
    }

    /**
     * 对于冷存文件进行激活操作
     * @param osMeta
     * @throws IOException
     */
    private void innerRestore(OSMeta osMeta) throws IOException {
        Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
        int retryCount = 1;
        try {
            UfileClient.object(objauth, objcfg)
                    .objectRestore(osMeta.getKey(), osMeta.getBucket())
                    .execute();
            return;
        } catch (UfileClientException e) {
            UFileUtils.Error(cfg.getLogLevel(),"[innerRestore] client, %s ", e.toString());
            if(retryCount >= Constants.DEFAULT_MAX_TRYTIMES)
            throw UFileUtils.TranslateException(String.format("[innerRestore] %s %s", osMeta.getBucket(), osMeta.getKey()), osMeta.getKey(), e);
        } catch (UfileServerException e) {
            UFileUtils.Error(cfg.getLogLevel(),"[innerRestore] server, %s ", e.toString());
            if(retryCount>=Constants.DEFAULT_MAX_TRYTIMES||e.getErrorBean().getResponseCode()<500)
            throw UFileUtils.TranslateException(String.format("[innerRestore] %s %s", osMeta.getBucket(), osMeta.getKey()),  osMeta.getKey(), e);
        }finally{
            retryCount ++;
            try {
                Thread.sleep(retryCount* Constants.TRY_DELAY_BASE_TIME);
            } catch (InterruptedException e) {
                throw new IOException("not able to handle exception", e);
            }
        }
    }

    public Map<String, String> extractUserMeta(String defOwner, String defGroup, String crc32c, FsPermission permission,
                                               long blockSize, short replication, String base64Md5) {
        Map<String, String> userMeta = new HashMap<>();
        userMeta.put(Constants.HDFS_PERMISSION_USER_KEY, UFileUtils.EncodeFsAction(permission.getUserAction()));
        userMeta.put(Constants.HDFS_PERMISSION_GROUP_KEY, UFileUtils.EncodeFsAction(permission.getGroupAction()));
        userMeta.put(Constants.HDFS_PERMISSION_OTHER_KEY, UFileUtils.EncodeFsAction(permission.getOtherAction()));
        userMeta.put(Constants.HDFS_PERMISSION_STICKY_KEY, UFileUtils.EncodeFsSticky(permission.getStickyBit()));
        userMeta.put(Constants.HDFS_REPLICATION_NUM_KEY, UFileUtils.EncodeReplication(replication));
        userMeta.put(Constants.HDFS_BLOCK_SIZE_KEY, UFileUtils.EncodeBlockSize(blockSize));
        if (defOwner == null) userMeta.put(Constants.HDFS_OWNER_KEY, username);
        else userMeta.put(Constants.HDFS_OWNER_KEY, defOwner);

        if (defOwner == null) userMeta.put(Constants.HDFS_GROUP_KEY, Constants.superGroup);
        else userMeta.put(Constants.HDFS_GROUP_KEY, defGroup);
        if (crc32c != null) userMeta.put(Constants.HDFS_CHECKSUM_KEY, crc32c);
        if (base64Md5 != null) userMeta.put(Constants.META_MD5_HASH, base64Md5);
        return userMeta;
    }

    @Override
    public boolean setReplication(Path src, short replication) throws IOException {
        UFileUtils.Debug(cfg.getLogLevel(),"[setReplication] f:%s' replication:%d ", src, replication);
        OSMeta osMeta = UFileUtils.ParserPath(uri, workDir, src);
        UFileFileStatus ufs = Constants.ufileMetaStore.getUFileFileStatus(cfg, osMeta.getKey());
        if (ufs == null || ufs.is404Cache()) {
            UFileUtils.Debug(cfg.getLogLevel(),"[setReplication] setXXX not set ufile status ");
            Constants.ufileMetaStore.removeUFileFileStatus(osMeta.getKey());
            ufs = innerGetFileStatus(src);
            if (ufs == null) {
                UFileUtils.Error(cfg.getLogLevel(),"[setReplication] ufile:%s status is empty after head again ", osMeta.getKey());
                return true;
            }
        }

        if (ufs.isDirectory()) {
            UFileUtils.Info(cfg.getLogLevel(),"[setReplication] src:%s is directory", src);
            return false;
        }

        String userName = ufs.getOverrideGroupName();
        String groupName = ufs.getOverrideGroupName();
        if (userName == null || userName.equals("")) userName = ufs.getOwner();
        if (groupName == null || groupName.equals("")) groupName = ufs.getGroup();
        Map<String, String> userMeta = extractUserMeta(userName,
                groupName,
                ufs.getHexCrc32c(),
                ufs.getPermission(),
                ufs.getBlockSize(),
                replication,
                ufs.getBase64Md5());
        replaceUserMeta(osMeta, ufs, userMeta);
        return true;
    }

    private boolean replaceUserMeta(OSMeta osMeta, UFileFileStatus ufs,
                                    Map<String, String> userMeta ) throws IOException {
        innerCopyFile(osMeta, osMeta, "REPLACE", userMeta);
        Constants.ufileMetaStore.putUFileFileStatusWithTimeout(cfg, osMeta.getKey(), ufs, 15);
        if (ufs.isDirectory()) {
            String key = osMeta.getKey();
            if (key.endsWith("/")) {
                osMeta.setKey(key.substring(key.length()-1));
            } else {
                osMeta.setKey(key+"/");
            }
            innerCopyFile(osMeta, osMeta, "REPLACE", userMeta);
            Constants.ufileMetaStore.putUFileFileStatusWithTimeout(cfg, osMeta.getKey(), ufs, 15);
        }
        return true;
    }

    private synchronized static Map<String, String> getDefaultUserMeta() {
        if (defaultUserMeta == null) {
            defaultUserMeta = new HashMap<>();
            defaultUserMeta.put(Constants.HDFS_PERMISSION_STICKY_KEY, "false");
            defaultUserMeta.put(Constants.HDFS_PERMISSION_USER_KEY, Constants.HDFS_FILE_READ_WRITE);
            defaultUserMeta.put(Constants.HDFS_PERMISSION_GROUP_KEY, Constants.HDFS_FILE_READ_WRITE);
            defaultUserMeta.put(Constants.HDFS_PERMISSION_OTHER_KEY, Constants.HDFS_FILE_READ_WRITE);
            FsPermission perm = UFileFileStatus.parsePermission(defaultUserMeta);
        }
        return defaultUserMeta;
    }

    @Override
    public long getDefaultBlockSize() {
        UFileUtils.Debug(cfg.getLogLevel(),"[getDefaultBlockSize] default");
        return Constants.DEFAULT_HDFS_BLOCK_SIZE;
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        UFileUtils.Debug(cfg.getLogLevel(),"[getDefaultBlockSize] get %s status", f.toString());
        try {
            UFileFileStatus ufs = innerGetFileStatus(f);
            if (ufs == null) { return Constants.DEFAULT_HDFS_BLOCK_SIZE; }
            else { return ufs.getBlockSize(); }
        } catch (IOException e) {
            UFileUtils.Error(cfg.getLogLevel(),"[getDefaultBlockSize] get %s status,  %s ", f.toString(), e.toString());
            return Constants.DEFAULT_HDFS_BLOCK_SIZE;
        }
    }

    public Configure getCfg() { return cfg; }

    public UfileObjectLocalAuthorization getAuth() { return objauth;}
    public ObjectConfig getMdsCfg() { return objcfg;}
}
