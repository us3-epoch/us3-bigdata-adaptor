package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.api.object.ObjectConfig;
import cn.ucloud.ufile.auth.UfileObjectLocalAuthorization;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;

public class UFileInputStreamTest {

    private static Configure cfg = new Configure();

    private static UfileObjectLocalAuthorization objAuthCfg =
            new UfileObjectLocalAuthorization(UFileFileSystemTest.AccessKey,
                    UFileFileSystemTest.SecretKey);

    private static ObjectConfig objCfg = new ObjectConfig(UFileFileSystemTest.Bucket + "." +
            UFileFileSystemTest.Endpoint);

    private static String bucket = UFileFileSystemTest.Bucket;

    private static long seek = 65;

    private static int readBufferSize = 1024*8;

    @Test
    public void seek() throws IOException {
        if (UFileFileSystemTest.SecretKey == null && UFileFileSystemTest.SecretKey.isEmpty()) return;
        UFileFileSystem ufs = UFileFileSystemTest.createUFileFs();
        FSDataOutputStream outputStream = ufs.create(new Path("us3-bigdata-test.file"));
        RandomAccessFile randomReadFile = new RandomAccessFile("src/main/java/cn/ucloud/ufile/fs/UFileFileSystem.java", "r");
        byte[] buf = new byte[readBufferSize];
        long sum = 0;
        while (true) {
            int count = randomReadFile.read(buf,0, buf.length);
            sum += count;
            if (count < 0) {
                System.out.println("read over, count is " + count + ", sum is " + sum);
                break;
            }
            outputStream.write(buf, 0, count);
        }
        outputStream.close();
        randomReadFile.close();

        byte[] buf1 = new byte[1024];
        byte[] buf2 = new byte[buf1.length];

        FSDataInputStream ufileStream = ufs.open(new Path("us3-bigdata-test.file"));
        System.out.printf("1\n");
        //ufileStream.read(buf1, 0, buf1.length-1);
        System.out.printf("2\n");
        ufileStream.seek(UFileInputStreamTest.seek);
        System.out.printf("3\n");
        int r1 = ufileStream.read(buf1, 0, buf1.length);
        System.out.printf("4\n");
        int c1 = ufileStream.read();
        System.out.printf("5\n");

        randomReadFile = new RandomAccessFile("src/main/java/cn/ucloud/ufile/fs/UFileFileSystem.java", "r");
        System.out.printf("6\n");
        //randomReadFile.read(buf2, 0, buf2.length);
        System.out.printf("7\n");
        randomReadFile.seek(UFileInputStreamTest.seek);
        System.out.printf("8\n");
        int r2 = randomReadFile.read(buf2, 0, buf2.length);
        System.out.printf("9\n");
        int c2 = randomReadFile.read();
        System.out.printf("10\n");

        assertEquals(c1, c2);

        if (r1 != r2) {
            assertEquals(r1, r2);
        }

        for (int i = 0; i < r1 ; i++ ){
            assertEquals(buf1[i], buf2[i]);
        }

        System.out.printf("11\n");
        ufileStream.seek(1024);
        System.out.printf("12\n");
        r1 = ufileStream.read(buf1, 0, buf1.length);
        System.out.printf("13\n");
        randomReadFile.seek(1024);
        System.out.printf("14\n");
        r2 = randomReadFile.read(buf2, 0, buf2.length);

        for (int i = 0; i < r1; i++ ){
            assertEquals(buf1[i], buf2[i]);
        }

        System.out.printf("15\n");
        ufileStream.close();
        System.out.printf("16\n");
        randomReadFile.close();
    }

    @Test
    public void getPos() {
    }

    @Test
    public void seekToNewSource() {
    }

    @Test
    public void read() {
    }

    @Test
    public void read1() {
    }

    @Test
    public void close() {
    }
}
