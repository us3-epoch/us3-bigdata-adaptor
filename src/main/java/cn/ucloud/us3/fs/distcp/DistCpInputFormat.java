package cn.ucloud.us3.fs.distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 16:51
 */

class DstValue implements Writable {
    public static String SUCC = "";
    private Text dst = new Text();
    private Text errMsg = new Text();

    DstValue(){}
    DstValue(String dst, String errMsg) {
        this.dst.set(dst);
        this.errMsg.set(errMsg);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dst.write(dataOutput);
        errMsg.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
         dst.readFields(dataInput);
         errMsg.readFields(dataInput);
    }

    public Path getDst() { return new Path(dst.toString()); }
    public String getErrMsg() { return errMsg.toString(); }
    public void setErrMsg(String errMsg) { this.errMsg.set(errMsg); }
}

public class DistCpInputFormat extends InputFormat<Text, DstValue> {
    public static Log LOG = LogFactory.getLog(DistCpInputFormat.class);
    private Path inputPath = null;
    private FileSystem inputPathFs = null;
    private String stage = null;
    private String checkSum = null;

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
     * @return
     * @throws IOException
     */
    private long countTotalSize(Configuration conf) throws IOException {
        return CopyListing.countTotalSize(conf, stage, checkSum, inputPath, false);
    }

    /**
     * 初始化stage、inputPath、inputPathFs
     * @param conf
     */
    private void prepare(Configuration conf) throws IOException {
        String input = null;
        stage = conf.get(DistCpConstants.CONF_LABEL_STAGE);
        checkSum = conf.get(DistCpConstants.CONF_LABEL_CHECKSUM, DistCpConstants.CHECKSUM_UNDO);
        switch (stage) {
            case DistCpConstants.STAGE_CHECK:
                input = conf.get(DistCpConstants.CONF_LABEL_CHECK_INPUT_DIR);
                break;
            case DistCpConstants.STAGE_CP:
                input = conf.get(DistCpConstants.CONF_LABEL_CP_INPUT_DIR);
                break;
            default:
                throw new IOException("stage"+ stage +" is unkown");
        }
        inputPath = new Path(input);
        inputPathFs = inputPath.getFileSystem(conf);
    }

    public List<InputSplit> getSplits(Configuration conf, int numSplits, long totalSize) throws IOException{
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        long sizePerSplit = (long)Math.ceil(totalSize*1.0/numSplits);

        Text src = new Text();
        DstValue dst = new DstValue();
        Long currentSplitSize = 0L;
        Long lastSplitStart = 0L;
        Long lastPosition = 0L;

        LOG.info("Average size of map: " + sizePerSplit + ", Number of maps:" + numSplits + ", total size:" + totalSize);

        FileStatus[] fss = inputPathFs.listStatus(inputPath);
        for (FileStatus fs: fss) {
            SequenceFile.Reader reader;
            try{
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fs.getPath()));
            } catch (EOFException e) {
                LOG.warn(fs.getPath().toString() + " read err:" + e.getMessage());
                continue;
            }

            boolean firstReaded = false;
            while (reader.next(src, dst)) {
                    long delta = 0;
                    Path srcPath = new Path(src.toString());
                    switch (stage) {
                    case DistCpConstants.STAGE_CHECK:
                        switch (checkSum) {
                            case DistCpConstants.CHECKSUM_ALL:
                                FileStatus fts = srcPath.getFileSystem(conf).getFileStatus(srcPath);
                                delta += fts.getLen();
                                break;
                            case DistCpConstants.CHECKSUM_RANDOME:
                                fts = srcPath.getFileSystem(conf).getFileStatus(srcPath);
                                if (fts.getLen() > DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE) {
                                    delta += DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE;
                                } else {
                                    delta += fts.getLen();
                                }
                                break;
                            case DistCpConstants.CHECKSUM_UNDO:
                                delta += 1;
                                break;
                        }
                        break;
                    case DistCpConstants.STAGE_CP:
                        FileStatus fts = srcPath.getFileSystem(conf).getFileStatus(srcPath);
                        delta += fts.getLen();
                        break;
                    }
                    LOG.debug("getSplits src: " + src.toString() + " delta:" + delta);

                    // 不能分片超过限定值
                    if (currentSplitSize + delta > sizePerSplit && lastPosition != 0) {
                        FileSplit split = new FileSplit(fs.getPath(), lastSplitStart, lastPosition-lastSplitStart, null);
                        LOG.debug("Create new split : " + split + ", size in split:" + String.valueOf(currentSplitSize));

                        splits.add(split);
                        lastSplitStart = lastPosition;
                        currentSplitSize = 0L;
                    }
                    currentSplitSize += delta;
                    lastPosition = reader.getPosition();
            }

            if (lastPosition > lastSplitStart) {
                FileSplit split = new FileSplit(fs.getPath(), lastSplitStart, lastPosition-lastSplitStart, null);
                splits.add(split);
                LOG.debug("Create new split : " + split + ", size in split:" + String.valueOf(currentSplitSize));
            }
            reader.close();
        }
        return splits;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration configuration = jobContext.getConfiguration();
        prepare(configuration);
        Long totalSize = configuration.getLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, -1);
        if (totalSize <= 0) {
            /**
             *  还未计算需要先扫描一遍计算
             */
            totalSize = countTotalSize(configuration);
            LOG.info("the size of files need count by DistInputFormat, size is " + Long.valueOf(totalSize));
        } else {
            LOG.info("the size of files count by DistCp, size is " + Long.valueOf(totalSize));
        }

        if (totalSize <= 0) {
            throw new IOException("total size is "+ totalSize);
        }

        int numSplits = configuration.getInt(org.apache.hadoop.mapred.JobContext.NUM_MAPS, DistCpConstants.DEFAULT_MAP_NUM);
        return getSplits(configuration, numSplits, totalSize);
    }

    @Override
    public RecordReader<Text, DstValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new SequenceFileRecordReader<Text, DstValue>();
    }
}
