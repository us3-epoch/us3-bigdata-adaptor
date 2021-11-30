package cn.ucloud.ufile.fs.tools;

import cn.ucloud.ufile.fs.Configure;
import cn.ucloud.ufile.fs.Constants;
import cn.ucloud.ufile.fs.UFileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

public class BuildHdfs {
    public static Path rootPathP;
    public static String endpoint;
    public static String accesskey;
    public static String secretkey;
    public static String loglevel = "info";
    public static Configure cfg = new Configure();

    private static Configuration config = new Configuration();
    private static FileSystem fs;

    private static void displayUsage() {
        String usage = "Usage: buildhdfs <options>\n" +
                "Options:\n" +
                "\t-ufilebucket <ufile bucket name>\n" +
                "\t-endpoint <ufile endpoint>\n" +
                "\t-accesskey <api public key or ufile public token>\n" +
                "\t-secretkey <api private key or ufile private token>\n" +
                "\t-loglevel <available level are debug info error>\n" +
                "\t-path <path eg: us3://us3-bucket/hellowold>\n";
                System.out.println(usage);
        System.exit(-1);
    }

    private static void displayVersion() {
        System.out.println("BuildHdfs From UFile 0.1");
        System.exit(-1);
    }

    public static void parseInputs(String[] args) {
        if (args.length == 0) {
            displayUsage();
        }

        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("-ufilebucket")) {
                ++i;
                String rootPath = "ufile://"+args[i];
                rootPathP = new Path(rootPath, "/");
            } else if (args[i].equals("-endpoint")) {
                ++i;
                endpoint = args[i];
            } else if (args[i].equals("-accesskey")) {
                ++i;
                accesskey = args[i];
            } else if (args[i].equals("-secretkey")) {
                ++i;
                secretkey = args[i];
            } else if (args[i].equals("-loglevel")) {
                ++i;
                loglevel = args[i];
            } else if (args[i].equals("-path")) {
                ++i;
                rootPathP = new Path(args[i]);
            } else if (args[i].equals("-help")) {
                displayUsage();
                System.exit(-1);
            }
        }

        if (rootPathP == null || endpoint == null || accesskey == null || secretkey == null ||
                (!loglevel.equals("info")) && !loglevel.equals("error") && !loglevel.equals("debug")) {
            displayUsage();
        }

        config.set(Constants.CS_ACCESS_KEY, accesskey);
        config.set(Constants.CS_US3_ACCESS_KEY, accesskey);

        config.set(Constants.CS_SECRET_KEY, secretkey);
        config.set(Constants.CS_US3_SECRET_KEY, secretkey);

        config.set(Constants.CS_ENDPOINT , endpoint);
        config.set(Constants.CS_US3_ENDPOINT, endpoint);

        config.set(Constants.CS_LOG_LEVEL_KEY, loglevel);
        config.set(Constants.CS_US3_LOG_LEVEL_KEY, loglevel);

        config.setInt(Constants.CS_SOCKET_RECV_BUFFER , 10*1024);
        config.setInt(Constants.CS_US3_SOCKET_RECV_BUFFER, 10*1024);

        config.set("fs.AbstractFileSystem.ufile.impl", "cn.ucloud.ufile.fs.UFileFs");
        config.set("fs.AbstractFileSystem.us3.impl", "cn.ucloud.us3.fs.US3Fs");

        config.set("fs.ufile.impl", "cn.ucloud.ufile.fs.UFileFileSystem");
        config.set("fs.us3.impl", "cn.ucloud.us3.fs.US3FileSystem");
        config.set("fs.ufile.log.level", loglevel);
        try {
            cfg.Parse(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createFs() {
        try {
            fs = (rootPathP).getFileSystem(config);
        } catch (IOException e ){
            e.printStackTrace();
        }
    }

    public static void createDir(Path path) {
        UFileUtils.Info(cfg.getLogLevel(), "[createDir] create dir for path:%s ", path);
        if (!path.toString().equals(rootPathP.toString())) {
            try {
                fs.getFileStatus(path);
            } catch (FileNotFoundException e) {
                try {
                    fs.mkdirs(path);
                } catch (IOException e1) {
                    UFileUtils.Error(cfg.getLogLevel(), "[createDir] mkdirs dir:%s io exception", path);
                    e.printStackTrace();
                    System.exit(-1);
                }
            } catch (IOException e) {
                UFileUtils.Error(cfg.getLogLevel(), "[createDir] getFileStatus file:%s io exception", path);
                e.printStackTrace();
                System.exit(-1);
            }
            UFileUtils.Info(cfg.getLogLevel(), "[createDir] path:%s exists, no need creat", path);
        }

        FileStatus[] fss = null;
        try {
            fss = fs.listStatus(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (fss == null || fss.length == 0) {
            UFileUtils.Info(cfg.getLogLevel(), "path(%s)'s sub dir is empty", path);
            return;
        }

        for (FileStatus fst: fss) {
            if (fst.isDirectory()) {
                UFileUtils.Debug(cfg.getLogLevel(), "[createDir] f:%s is %s's sub dir", fst.getPath(), path);
                createDir(fst.getPath());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        parseInputs(args);
        createFs();
        createDir(rootPathP);
    }
}
