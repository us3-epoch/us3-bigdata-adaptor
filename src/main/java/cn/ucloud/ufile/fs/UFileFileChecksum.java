package cn.ucloud.ufile.fs;

import org.apache.hadoop.fs.FileChecksum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UFileFileChecksum extends FileChecksum {
    private byte[] result = new byte[4];

    public UFileFileChecksum(String hexCrc32c) {
        long value = Long.valueOf(hexCrc32c, 16);
        for (int i = 3; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
    }

    @Override
    public String getAlgorithmName() { return Constants.CHECKSUM_ALGORITHM_NAME; }

    @Override
    public int getLength() { return result.length; }

    @Override
    public byte[] getBytes() { return result; }

    /**
     * no need to implements
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException { return; }

    /**
     * no need to implements
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException { return; }
}
