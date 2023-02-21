package cn.ucloud.us3.fs.distcp;

import cn.ucloud.ufile.fs.common.Crc32c;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class DigitalSignatureAlgorithm {
    private Crc32c crc32c = null;
    private MessageDigest digest = null;

    public DigitalSignatureAlgorithm(Crc32c crc32c) { this.crc32c = crc32c;}
    public DigitalSignatureAlgorithm(MessageDigest digest) { this.digest = digest;}

    public void update(byte[] bArray, int off, int len) {
        if (crc32c != null) crc32c.update(bArray, off, len);
        else digest.update(bArray, off, len);
    }

    public void reset() {
        if (crc32c != null) crc32c.reset();
        else digest.reset();
    }

    public String toString() {
        if (crc32c != null) return Long.toHexString(crc32c.getValue());
        else return new String(digest.digest());
    }

    public String getName() {
        if (crc32c != null) return "crc32";
        else return "digest";
    }
}

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 13:59
 */
public class DistCpMapper extends Mapper<Text, DstValue, Text, DstValue> {
    private Log LOG = LogFactory.getLog(DistCpMapper.class);
    private Configuration conf;
    private boolean checkLastModtime;
    private boolean skipCheck;
    private Long checkSumRandomMaxSize;
    private String stage;
    private String checkSum;
    private String checkSumAlogrithm;
    private DigitalSignatureAlgorithm dsa;
    private byte[] readBuf;

    enum US3_DISTCP_CHECK {
        TOTAL_NUMBER_OF_INPUT_FILES,
        NUMBER_OF_DST_NOT_FOUND,
        NUMBER_OF_SIZE_NO_MATCH,
        NUMBER_OF_LASTMODIFY_NO_MATCH,
        NUMBER_OF_CHECKSUM_NO_MATCH,
        SIZE_OF_SOURCE_CHECKSUM_READ,
        SIZE_OF_DISTINATION_CHECKSUM_READ,
    }

    enum US3_DISTCP_COPY {
        TOTAL_NUMBER_OF_INPUT_FILES,
        NUMBER_OF_COPY_FAILURE,
        READ_SIZE_OF_SOURCE,
        WRITE_SIZE_OF_DISTINATION,
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        checkLastModtime = conf.getBoolean(DistCpConstants.CONF_LABEL_MODTIME, false);
        skipCheck = conf.getBoolean(DistCpConstants.CONF_LABEL_SKIP_CHECK, false);
        stage = conf.get(DistCpConstants.CONF_LABEL_STAGE);
        switch (stage) {
            case DistCpConstants.STAGE_CHECK:
                checkSum = conf.get(DistCpConstants.CONF_LABEL_CHECKSUM, DistCpConstants.CHECKSUM_UNDO);
                checkSumAlogrithm = conf.get(DistCpConstants.CONF_LABEL_CHECKSUM_ALGORITHM, DistCpConstants.CHECKSUM_ALGORITHM_CRC32C);
                switch (checkSumAlogrithm) {
                    case DistCpConstants.CHECKSUM_ALGORITHM_CRC32C:
                        dsa = new DigitalSignatureAlgorithm(new Crc32c());
                        break;
                    case DistCpConstants.CHECKSUM_ALGORITHM_MD5:
                        try {
                            dsa = new DigitalSignatureAlgorithm(MessageDigest.getInstance("MD5"));
                        } catch (NoSuchAlgorithmException e) {
                            throw new IOException(e);
                        }
                        break;
                }
        }
        readBuf = new byte[DistCpConstants.CHECKSUM_READ_BUFFER_SIZE];
    }

    private String getnCheckSum(FSDataInputStream stream, long offset , long len) throws IOException {
        dsa.reset();
        stream.seek(offset);
        Long sum = 0L;
        int off = 0;
        int countPerRead = 0;
        int length = readBuf.length > len ? (int)len: readBuf.length;
        boolean printf = false;
        while (sum < len) {
            countPerRead = stream.read(readBuf, off, length-off);
            if (countPerRead < 0) {
                // EOF
                break;
            } else if (countPerRead > 0) {
                off += countPerRead;
                sum += countPerRead;
            }

            if (off >= 30 && !printf) {
                LOG.debug("content:" + new String(readBuf, 0, 30));
                printf = true;
            }

            if (off >= length) {
                // Read full, caculate checksum
                dsa.update(readBuf, 0, length);
                // RESET READBUFF
                off = 0;
            }
        }

        if (off > 0) dsa.update(readBuf, 0, off);
        return dsa.toString();
    }

    private boolean checkSumSucc(Context context, FileSystem srcFs, FileStatus src,FileSystem dstFs, FileStatus dst) throws IOException{
        FSDataInputStream srcInputStream = srcFs.open(src.getPath());
        FSDataInputStream dstInputStream = dstFs.open(dst.getPath());
        Long[] offsets = new Long[6];
        offsets[1] = 0L;
        Long[] lens = new Long[6];
        lens[1] = 0L;
        switch (checkSum) {
            case DistCpConstants.CHECKSUM_ALL:
                offsets[0] = 0L;
                lens[0] = src.getLen();
                break;
            case DistCpConstants.CHECKSUM_RANDOME:
                if (src.getLen() <= DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE) {
                    offsets[0] = 0L;
                    lens[0] = src.getLen();
                } else {
                    offsets[0] = 0L;
                    lens[0] = DistCpConstants.CHECKSUM_RANDOME_HEAD_SIZE;

                    offsets[5] = src.getLen()-DistCpConstants.CHECKSUM_RANDOME_TAIL_SIZE;
                    lens[5] = DistCpConstants.CHECKSUM_RANDOME_HEAD_SIZE;

                    Long min = DistCpConstants.CHECKSUM_RANDOME_HEAD_SIZE+1;
                    Long max = offsets[5]-1;
                    offsets[1] = min + (new Double(Math.random()* (max-min+1)).longValue());
                    lens[1] = DistCpConstants.CHECKSUM_RANDOME_MIDDLE_SIZE;

                    offsets[2] = min + (new Double(Math.random()* (max-min+1)).longValue());
                    lens[2] = DistCpConstants.CHECKSUM_RANDOME_MIDDLE_SIZE;

                    offsets[3] = min + (new Double(Math.random()* (max-min+1)).longValue());
                    lens[3] = DistCpConstants.CHECKSUM_RANDOME_MIDDLE_SIZE;

                    offsets[4] = min + (new Double(Math.random()* (max-min+1)).longValue());
                    lens[4] = DistCpConstants.CHECKSUM_RANDOME_MIDDLE_SIZE;
                }
                break;
            default:
        }

        boolean equality = true;
        long sourceRead = 0;
        long distinationRead = 0;
        for (int i = 0; i < offsets.length; i++) {
            if (i >= 1 && lens[i] == 0L) {
                break;
            }

            switch (checkSumAlogrithm) {
                case DistCpConstants.CHECKSUM_ALGORITHM_CRC32C:
                case DistCpConstants.CHECKSUM_ALGORITHM_MD5:
                    sourceRead += lens[i];
                    distinationRead = sourceRead;
                    String srcChecksum = getnCheckSum(srcInputStream, offsets[i], lens[i]);
                    String dstChecksum = getnCheckSum(dstInputStream, offsets[i], lens[i]);
                    equality = srcChecksum.equals(dstChecksum);
                    if (!equality) {
                        LOG.error(i + ". "+ dsa.getName() +" checksum src:" + src.getPath().toString() + "("+ srcChecksum +") => dst:"
                                + dst.getPath().toString() + "("+ dstChecksum +") not match!! offset:" + Long.valueOf(offsets[i]) + " len:" + Long.valueOf(lens[i]));
                    } else {
                        LOG.info(i + ". "+ dsa.getName() +" checksum src:" + src.getPath().toString() + "("+ srcChecksum +") => dst:"
                                + dst.getPath().toString() + "("+ dstChecksum +") match ^_^");
                    }
                    break;
                default:
                    throw new IOException("unknow checksum alogrithm:" + checkSumAlogrithm);
            }

            if (!equality) {
                break;
            }
        }

        context.getCounter(US3_DISTCP_CHECK.SIZE_OF_SOURCE_CHECKSUM_READ).increment(sourceRead);
        context.getCounter(US3_DISTCP_CHECK.SIZE_OF_DISTINATION_CHECKSUM_READ).increment(distinationRead);
        return equality;
    }

    private void setAttr(FileSystem srcFs, FileStatus src,FileSystem dstFs, Path dst) throws IOException {
        dstFs.setOwner(dst, src.getOwner(), src.getGroup());
        dstFs.setPermission(dst, src.getPermission());
    }

    private boolean copyV2(FileSystem srcFs, FileStatus src,FileSystem dstFs, Path dst) throws IOException {
        FSDataInputStream inputStream = srcFs.open(src.getPath());
        FSDataOutputStream outStream = dstFs.create(dst, src.getPermission(),
                true ,readBuf.length,  (short)3,
                src.getBlockSize(), null);

        Long srcLen = src.getLen();
        Long sum = 0L;
        int off = 0;
        while (true) {
            int countPerRead = 0;
            try {
                countPerRead = inputStream.read(readBuf, 0, readBuf.length);
                if (countPerRead < 0 ){ break; }
                if (countPerRead != 0) { outStream.write(readBuf, 0, countPerRead); }
            } catch (IOException e) {
                LOG.error(e);
                inputStream.close();
                outStream.close();
                throw new IOException("read src:" + src.getPath().toString() + " write to:"+ dst.toString() + " failure");
            }
        }
        inputStream.close();
        outStream.close();
        return true;
    }

    private boolean copy(FileSystem srcFs, FileStatus src,FileSystem dstFs, Path dst) throws IOException {
        boolean succ = dstFs.delete(dst, true);
        if (!succ) {
            throw new IOException("delete " + dst.toString() + " failure");
        }

        FSDataInputStream inputStream = srcFs.open(src.getPath());
        FSDataOutputStream outStream = dstFs.create(dst, src.getPermission(),
                true ,readBuf.length,  (short)3,
                src.getBlockSize(), null);

        Long srcLen = src.getLen();
        Long sum = 0L;
        int off = 0;
        while (sum < srcLen) {
            int countPerRead = 0;
            try {
                countPerRead = inputStream.read(readBuf, off, readBuf.length-off);
            } catch (IOException e) {
                LOG.error(e);
                inputStream.close();
                outStream.close();
                throw new IOException("read src:" + src.getPath().toString() + " failure");
            }

            if (countPerRead < 0) {
                // EOF
                break;
            } else if (countPerRead > 0) {
                off += countPerRead;
                sum += countPerRead;
            } else {
                continue;
            }

            if (off >= readBuf.length) {
                // Read full, send
                try{
                    outStream.write(readBuf, 0, off);
                } catch (IOException e) {
                    LOG.error(e);
                    inputStream.close();
                    outStream.close();
                    throw new IOException("write dst:" + dst.toString() + " failure");
                }
                // RESET READBUFF
                off = 0;
            }
        }

        if (off > 0) {
            try {
                outStream.write(readBuf, 0, off);
            } catch (IOException e) {
                LOG.error(e);
                inputStream.close();
                outStream.close();
                throw new IOException("write dst:" + dst.toString() + " failure");
            }
        }
        inputStream.close();
        outStream.close();
        return true;
    }

    @Override
    protected void map(Text key, DstValue value, Context context) throws IOException, InterruptedException {
        Path srcPath = new Path(key.toString());
        Path dstPath = value.getDst();
        FileStatus srcFts;
        FileSystem srcFs = srcPath.getFileSystem(conf);
        FileStatus dstFts;
        FileSystem dstFs = dstPath.getFileSystem(conf);
        try {
            srcFts = srcFs.getFileStatus(srcPath);
        } catch (FileNotFoundException e) {
            value.setErrMsg("src not found");
            context.write(key, value);
            return;
        }

        switch (stage) {
        case DistCpConstants.STAGE_CHECK:
            context.getCounter(US3_DISTCP_CHECK.TOTAL_NUMBER_OF_INPUT_FILES).increment(1);
            LOG.info("attempId:" + context.getTaskAttemptID().toString() + ", check " + srcFts.getPath().toString() + " => " + dstPath.toString() + "!!");
            try {
                dstFts = dstFs.getFileStatus(dstPath);
            } catch (FileNotFoundException e) {
                value.setErrMsg("dst not found");
                context.getCounter(US3_DISTCP_CHECK.NUMBER_OF_DST_NOT_FOUND).increment(1);
                context.write(key, value);
                return;
            }

            if (srcFts.getLen() != dstFts.getLen()) {
                value.setErrMsg("size not match");
                context.getCounter(US3_DISTCP_CHECK.NUMBER_OF_SIZE_NO_MATCH).increment(1);
                context.write(key, value);
                return;
            }

            if (checkLastModtime) {
                if (srcFts.getModificationTime() > dstFts.getModificationTime()) {
                    value.setErrMsg("modification time not match");
                    context.getCounter(US3_DISTCP_CHECK.NUMBER_OF_LASTMODIFY_NO_MATCH).increment(1);
                    context.write(key, value);
                    return;
                }
            }

            if (!checkSum.equals(DistCpConstants.CHECKSUM_UNDO) && !checkSumSucc(context, srcFs, srcFts, dstFs, dstFts)) {
                value.setErrMsg("checksum not match");
                context.getCounter(US3_DISTCP_CHECK.NUMBER_OF_CHECKSUM_NO_MATCH).increment(1);
                context.write(key, value);
                return;
            }
            break;
        case DistCpConstants.STAGE_CP:
            context.getCounter(US3_DISTCP_COPY.TOTAL_NUMBER_OF_INPUT_FILES).increment(1);
            int tryTimes = 0;
            while (true) {
                try {
                    //int x = (int)(Math.random()*100);
                    //if (x > 50 ) {
                    //copy(srcFs, srcFts, dstFs, dstPath);
                    copyV2(srcFs, srcFts, dstFs, dstPath);
                    context.getCounter(US3_DISTCP_COPY.READ_SIZE_OF_SOURCE).increment(srcFts.getLen());
                    context.getCounter(US3_DISTCP_COPY.WRITE_SIZE_OF_DISTINATION).increment(srcFts.getLen());
                    LOG.info("attempId:" + context.getTaskAttemptID().toString() + ", cp " + srcFts.getPath().toString() + " => " + dstPath.toString() + " succ!! tryTimes:" + tryTimes);
                    //} else {
                    //    LOG.error("attempId:" + context.getTaskAttemptID().toString() + ", cp " + srcFts.getPath().toString() + " => " + dstPath.toString() + " simulation failure!!");
                    //    value.setErrMsg("simulation failure");
                    //    context.write(key, value);
                    //}
                    break;
                } catch (IOException e) {
                    LOG.error(e);
                    Thread.sleep(tryTimes*500);
                    tryTimes++;
                    if (tryTimes > 3) {
                        value.setErrMsg(e.getMessage().substring(0, 30));
                        context.write(key, value);
                        context.getCounter(US3_DISTCP_COPY.NUMBER_OF_COPY_FAILURE).increment(1);
                        break;
                    }
                    return;
                }
            }

            tryTimes = 0;
            while (true) {
                try {
                    setAttr(srcFs, srcFts, dstFs, dstPath);
                    LOG.info("attempId:" + context.getTaskAttemptID().toString() + ", set attr " + srcFts.getPath().toString() + " => " + dstPath.toString() + " succ!! tryTimes:" + tryTimes);
                    break;
                } catch (IOException e) {
                    LOG.error(e);
                    Thread.sleep(tryTimes*500);
                    tryTimes++;
                    if (tryTimes > 3) {
                        value.setErrMsg(e.getMessage().substring(0, 30));
                        context.write(key, value);
                        context.getCounter(US3_DISTCP_COPY.NUMBER_OF_COPY_FAILURE).increment(1);
                        break;
                    }
                    return;
                }
            }
            break;
        default:
        }
    }
}
