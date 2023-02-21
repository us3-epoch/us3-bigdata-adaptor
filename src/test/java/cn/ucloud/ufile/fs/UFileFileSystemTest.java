package cn.ucloud.ufile.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class UFileFileSystemTest {
    public static String Scheme = "us3";
    public static String Bucket = "xxx";
    public static String Endpoint = "xxx";
    public static String AccessKey = "xxx";
    public static String SecretKey = "xxx";
    public static String LogLvl = "debug";


    public static UFileFileSystem createUFileFs() throws IOException{
        UFileFileSystem fs = new UFileFileSystem();
        URI uri;
        try {
            uri = new URI(Scheme+"://"+Bucket+"/");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        Configuration cfg = new Configuration();

        cfg.set("fs.us3.access.key", AccessKey);
        cfg.set("fs.us3.secret.key", SecretKey);
        cfg.set("fs.us3.endpoint", Endpoint);
        cfg.set("fs.us3.log.level", LogLvl);
        cfg.setBoolean("fs.us3.metadata.use", false);
        cfg.set(Constants.CS_UFILE_MDS_ZOOKEEPER_ADDRS, "10.9.72.188");
        cfg.setBoolean("fs.client.resolve.remote.symlinks", true);
        fs.initialize(uri, cfg);
        return fs;
    }

    @Test
    public void create() throws IOException {
        if (SecretKey == null && SecretKey.isEmpty()) return;
        UFileFileSystem fs = createUFileFs();
        FSDataOutputStream out = fs.create(new Path(Scheme+"://"+Bucket+"/a/b/c/d/e/f/g"));
        out.write(1);
        out.write(2);
        out.write(3);
        out.close();
    }

    @Test
    public void rename() throws IOException {
        if (SecretKey == null && SecretKey.isEmpty()) return;
        UFileFileSystem fs = createUFileFs();
        Path dst = new Path(Scheme+"://"+Bucket+"/a/b/x");
        fs.delete(dst);
        assertEquals(fs.rename(new Path(Scheme+"://"+Bucket+"/a/b/c/d/e/f/g"), dst ), true);
    }

    @Test
    public void testDirPath() throws IOException {
        if (SecretKey == null && SecretKey.isEmpty()) return;
        UFileFileSystem fs = createUFileFs();
        Path p = new Path(Scheme+"://"+Bucket+"/", "user/7/a/b/c/d/");
        System.out.println(p.toString());
        assertEquals(Scheme+"://"+Bucket+"/user/7/a/b/c/d", p.toString());
    }

    @Test
    public void stat() throws IOException {
        if (SecretKey == null && SecretKey.isEmpty()) return;
        UFileFileSystem fs = createUFileFs();
        Path p = new Path(Scheme+"://"+Bucket+"/", "user/7/a/b/c/d/");
        try {
            FileStatus fss = fs.getFileStatus(p);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

}