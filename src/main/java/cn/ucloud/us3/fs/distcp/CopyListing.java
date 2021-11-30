package cn.ucloud.us3.fs.distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 16:00
 */
public class CopyListing {
    private static Log LOG = LogFactory.getLog(CopyListing.class);

    private Configuration configuration;

    CopyListing(Configuration conf) {
        configuration = conf;
    }

    public void buildingInputFromListing(Path listing, Path file) {
        // TODO
    }

    public static String getRelativePath(Path sourceRootPath, Path childPath) {
        String childPathString = childPath.toUri().getPath();
        String sourceRootPathString = sourceRootPath.toUri().getPath();
        return sourceRootPathString.equals("/") ? childPathString :
                childPathString.substring(sourceRootPathString.length());
    }

    /**
     * 遍历dir目录下所有节点并构建源到目的的结构
     * @param rootFs  拷贝源的文件系统
     * @param writer  构建的源到目的的序列文件
     * @param sourceRoot 最先输入的拷贝源
     * @param fts   需要处理的当前目录
     * @param target    目标路径，当前都是假设target为目录且存在
     * @throws IOException
     */
    public Long traverDirectory(String stage, String checksum, FileSystem rootFs, SequenceFile.Writer writer,
                                Path sourceRoot, FileStatus fts, Path target)
        throws IOException{
        if (fts.isFile()) {
            Text src = new Text(fts.getPath().toString());
            DstValue dst = new DstValue(target.toString()+ getRelativePath(sourceRoot, fts.getPath()), DstValue.SUCC);
            writer.append(src, dst);
            writer.sync();
            long countSize = countSize(stage, checksum, fts);
            LOG.debug("src:" + src.toString() + " dst:"+ dst.getDst().toString() + " size:" + fts.getLen() + " countSize:" + countSize);
            return countSize;
        }

        Long count = 0L;
        FileStatus[] fss = rootFs.listStatus(fts.getPath());
        for (FileStatus tmpFts: fss) {
            if (tmpFts.isFile()) {
                Text src = new Text(tmpFts.getPath().toString());
                DstValue dst = new DstValue(target.toString()+ getRelativePath(sourceRoot, tmpFts.getPath()), DstValue.SUCC);
                long countSize = countSize(stage, checksum, tmpFts);
                LOG.debug("src:" + src.toString() + " dst:"+ dst.getDst().toString() + " size:" + tmpFts.getLen() + " countSize:" + countSize);
                count += countSize;
                writer.append(src, dst);
                writer.sync();
            } else if (tmpFts.isDirectory()) {
                count += traverDirectory(stage, checksum, rootFs, writer, sourceRoot, tmpFts, target);
            }
        }

        return count;
    }

    /**
     * size有以下含义：
     *      1. 如果在check阶段，不校验checksum，那么就是文件个数；
     *      2. 如果在check阶段，校验checksum，那么就是文件字节数，但这里根据校验范围又分为2种:
     *          A. random
     *              i. <= (4+1+1+1+1+4)*4M大小的文件都算整个文件大小;
     *              ii. >= (4+1+1+1+1+4)*4M大小的文件都算(4+1+1+1+1+4)*4M大小;
     *          B. all
     *              i. 算整个文件大小
     *      3. 如果在cp阶段，计算文件字节数:
     * @param conf
     * @param stage
     * @param checkSum
     * @param path
     * @return
     * @throws IOException
     */
    public static long countTotalSize(Configuration conf, String stage, String checkSum, Path path, boolean forCheck) throws IOException {
        Long count = 0L;
        Text src = new Text();
        DstValue dst = new DstValue();
        FileStatus[] fss = path.getFileSystem(conf).listStatus(path);
        for (FileStatus fs: fss) {
            SequenceFile.Reader reader;
            try {
                reader = new SequenceFile.Reader(conf,
                        SequenceFile.Reader.file(fs.getPath()));
            } catch (EOFException e) {
                LOG.warn(fs.getPath().toString() + " read err:" + e.getMessage());
                continue;
            }

            while (reader.next(src, dst)) {
                Path srcPath = new Path(src.toString());
                FileStatus srcFts = srcPath.getFileSystem(conf).getFileStatus(srcPath);
                count += CopyListing.countSize(stage, checkSum, srcFts);
                if (forCheck && count > 0) {
                    break;
                }
            }
            reader.close();
            if (forCheck && count > 0) {
                break;
            }
        }
        return count;
    }
    /**
     *
     * @param stage 当前要做那种job，"check" or "cp"
     * @param checkSum  校验的模式，不校验:"undo", 全部校验:"all", 部分随机校验:"randome"
     * @param fss   需要判断文件的信息
     * @return
     */
    public static long countSize(String stage, String checkSum, FileStatus fss) {
        switch (stage) {
            case DistCpConstants.STAGE_CHECK:
                switch (checkSum) {
                    case DistCpConstants.CHECKSUM_ALL:
                        return fss.getLen();
                    case DistCpConstants.CHECKSUM_RANDOME:
                        if (fss.getLen() > DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE ) {
                            return DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE;
                        } else {
                            return fss.getLen();
                        }
                    case DistCpConstants.CHECKSUM_UNDO:
                        return 1L;
                }
                break;
            case DistCpConstants.STAGE_CP:
                return fss.getLen();
        }
        return 0L;
    }

    /**
     * @param sources
     * @param target
     * @param file
     * @return count of regular file
     * @throws IOException
     */
    public Long buildingInputFromSourcesAndTarget(String stage, String checksum, List<Path> sources, Path target, Path file) throws IOException {
        if (sources.size() == 0) {
            throw new IOException("sources is empty");
        }

        LOG.info("check input file:" + file.toString());
        Long count = 0L;
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(DstValue.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
        FileStatus targetFs = null;
        try {
            targetFs = target.getFileSystem(configuration).getFileStatus(target);
        } catch (FileNotFoundException e) {
        }

        boolean hashManySourceRegularFile = false;
        for (Path sourceRoot: sources) {
            FileStatus fss = sourceRoot.getFileSystem(configuration).getFileStatus(sourceRoot);
            if (fss.isFile()) {
                if (targetFs == null || (targetFs != null && targetFs.isFile())) {
                    if (hashManySourceRegularFile) {
                        LOG.error("multi source is regular file to only one dst regular file, src:" + sourceRoot.toString() + " dst:"+ target.toString());
                        throw new IOException("multi source is regular file to only one dst regular file, src:" + sourceRoot.toString() + " dst:"+ target.toString());
                    }
                    Text src = new Text(sourceRoot.toString());
                    DstValue dst = new DstValue(target.toString(), DstValue.SUCC);
                    writer.append(src, dst);
                    writer.sync();
                    long countSize = countSize(stage, checksum, fss);
                    LOG.info("src:" + src.toString() + " dst:"+ dst.getDst().toString() + " size:" + fss.getLen() + " countSize:" + countSize);
                    count += countSize;
                    /** 不能多个普通文件拷贝到同一个普通文件上 **/
                    hashManySourceRegularFile = true;
                    continue;
                }
            }

            FileSystem rootFs = sourceRoot.getFileSystem(configuration);
            FileStatus[] fsss = sourceRoot.getFileSystem(configuration).listStatus(sourceRoot);
            for (FileStatus fts: fsss) {
                count += traverDirectory(stage, checksum, rootFs, writer, sourceRoot, fts, target);
            }
        }
        writer.hflush();
        writer.close();
        return count;
    }
}
