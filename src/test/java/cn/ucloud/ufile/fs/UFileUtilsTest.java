package cn.ucloud.ufile.fs;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class UFileUtilsTest {

    @Test
    public void parserLogLevel() {
        assertEquals(LOGLEVEL.DEBUG, UFileUtils.ParserLogLevel("debug"));
        assertEquals(LOGLEVEL.DEBUG,UFileUtils.ParserLogLevel( "Debug"));
        assertEquals(LOGLEVEL.DEBUG,UFileUtils.ParserLogLevel( "DEBUG"));
        assertEquals(LOGLEVEL.INFO, UFileUtils.ParserLogLevel("info"));
        assertEquals(LOGLEVEL.INFO, UFileUtils.ParserLogLevel("Info"));
        assertEquals(LOGLEVEL.INFO, UFileUtils.ParserLogLevel("INFO"));
        assertEquals(LOGLEVEL.ERROR,UFileUtils.ParserLogLevel( "error"));
        assertEquals(LOGLEVEL.ERROR,UFileUtils.ParserLogLevel( "Error"));
        assertEquals(LOGLEVEL.ERROR,UFileUtils.ParserLogLevel( "ERROR"));
    }

    @Test
    public void parserPath() throws URISyntaxException {
        URI uri = new URI("ufile://epoch-wff-2/");
        Path workDir = new Path("ufile://epoch-wff-2/");
        Path now = new Path("ufile://epoch-wff-2/hello/world");
        assertEquals(new OSMeta("epoch-wff-2", "hello/world"), UFileUtils.ParserPath(uri, workDir, now));
    }

    @Test
    public void isDirectory() {
        assertEquals(true, UFileUtils.IsDirectory("hello/world/", 0));
        assertEquals(false, UFileUtils.IsDirectory("hello/world", 0));
        assertEquals(false, UFileUtils.IsDirectory("", 0));
        assertEquals(false, UFileUtils.IsDirectory("hello/world/", 1));
        assertEquals(false, UFileUtils.IsDirectory("hello/world", 1));
        assertEquals(false, UFileUtils.IsDirectory("", 1));
    }

    @Test
    public void ParserRestore() {
        try {
           ObjectRestoreExpiration ore = UFileUtils.ParserRestore("ongoing-request=\"false\", expiry-date=\"Tue, 24 Dec 2019 07:13:15 GMT\"");
           System.out.println(ore.onGoing);
           System.out.println(ore.expiration);
           Assert.assertTrue("相等", ore.onGoing == false);
           assertEquals( 1577171595, ore.expiration);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void log4jTest() {
        Logger LOG = LoggerFactory.getLogger(UFileUtilsTest.class);
        BasicConfigurator.configure();

        LOG.debug("I am {}, age is {}", "rick.wu", 18);
    }
}