package cn.ucloud.ufile.fs;

import java.io.IOException;
import java.io.InputStream;

public class BufInputStream extends InputStream {
    /** 对应的数据流，这里应该是与UFile的TCP流 */
    private InputStream in;

    /** 用来缓存预读的数据 */
    private byte[] buf;

    /** 已经从in中读入到buf中的总数据量有多少 */
    private int count = 0;

    /** buf的读偏移 */
    private int readOffset = 0;

    /** 流in是否读取完毕，既是否已经碰到EOF */
    private boolean finish = false;

    /** 该流是否读取完毕，前提是finish为True，且buf消费完(count == readOffset) */
    //private boolean closed = false;

    /** 已经被消费过的字节数 */
    //private int hasConsumed = 0;

    /** 已经被消费过的字节数 */
    private byte[] oneByte = null;

    private LOGLEVEL loglevel;

    public BufInputStream(LOGLEVEL loglevel, InputStream in, int bufSize) {
        this.loglevel = loglevel;
        this.in = in;
        this.buf = new byte[bufSize];
    }

    @Override
    public int available() throws IOException {
        return this.availableInBuff();
    }

    public int availableInBuff() throws IOException {
        /** 不用buf的长度来计算是因为，有可能流的长度不一定是buf的整数倍，最后一段可能未填满buf*/
        int available = count - readOffset;
        if (available < 0) throw new IOException("[BufInputStream.availableInBuff] count is smaller than readOffset");
        return available;
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }

    @Override
    public synchronized int read() throws IOException {
        if (oneByte == null) {
            oneByte = new byte[1];
        }
        int rs = read(oneByte, 0, 1);
        /** 这里如果返回0，是返回-1告诉其结尾吗? 理论上底层read不会返回0*/
        if (-1 == rs) { return -1; }
        if (0 == rs) throw new IOException("[BufInputStream.read] read zero byte");
        final int i = oneByte[0] & 0XFF;
        return i;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || b.length < len + off)
            throw new IndexOutOfBoundsException();

        this.fill();

        final int canRead = this.availableInBuff();
        if (canRead > 0) {
            int totalBytesRead = Math.min(canRead, len);
            System.arraycopy(buf, this.readOffset, b, off, totalBytesRead);
            this.readOffset += totalBytesRead;
            //this.hasConsumed += totalBytesRead;
            return totalBytesRead;
        }

        if (this.finish) {
            return -1;
        }

        return 0;
    }

    /**
     * 真正发生IO操作的方法，它会从TCP流中获取字节数据填满buf
     * 只有当buf被消耗完时才会对in进行读取操作，这样可以集中read操作，发挥UFile的吞吐能力
     * @throws IOException
     */
    private void fill() throws IOException {
        if (!this.finish && this.count == this.readOffset) {
            int readSum = 0;
            int offSet = 0;
            while (readSum < this.buf.length) {
                int c = in.read(this.buf, offSet, this.buf.length - readSum);
                /*UFileUtils.Trace(loglevel, "[BufInputStream.fill] offset:%d buf.len:%d readSum:%d c:%d",
                        offSet,
                        this.buf.length,
                        readSum,
                        c);*/
                if (-1 == c) {
                    this.finish = true;
                    break;
                } else if (0 != c) {
                    offSet += c;
                    readSum += c;
                }
            }
            this.count = readSum;
            this.readOffset = 0;
        }
    }

    /**
     * 这里覆盖父类的skip方法，因为数据流的一部分数据提前预先提取到buf中，可能只需在buf中移动偏移即可
     * @param n 需要skip的字节数
     * @return  返回实际skip的字节数
     * @throws IOException
     */
    @Override
    public long skip(long n) throws IOException {
        /** 实际skip过的字节数 */
        long skipCount = 0;
        while (true) {
            if (isClosed()) {
                return skipCount;
            }

            long avaiCount = available();
            if (n <= avaiCount) {
                /** 如果n小于剩余未消费的buf，那么只需要对buf的偏移进行操作即可*/
                readOffset += n;
                skipCount += n;
                return skipCount;
            } else {
                /** 如果还没有填充数据，先从流in中读取填充buf */
                if (avaiCount <= 0) {
                    fill();
                    /** 因为有可能重新读取过后，buf里的数据会超过剩余需要skip的n个字节数*/
                    continue;
                }
                skipCount += avaiCount;
                /** 因为是一次性消费掉buf中的数据，所以buf的read偏移直接置换到buf数据的末尾 **/
                readOffset = count;
                n -= avaiCount;
            }
        }
    }

    /**
     * 该方法来判断该流是否已经结束，因为in的消费碰到EOF，不代表上层
     * 已经从当前流中完全消费了，因为有可能在buf中的数据还没有被上层
     * 消费，所以判断关闭的条件为两个:
     * 1. buf里没有可以消费的数据
     * 2. 流in已经读取到文件结尾
     * @return
     */
    private boolean isClosed() throws IOException {
        if (available() <= 0 && finish) {
            return true;
        }
        return false;
    }
}
