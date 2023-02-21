package cn.ucloud.ufile.fs.tools;

import cn.ucloud.ufile.fs.UFileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Name: cn.ucloud.ufile.fs.tools
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 16:18
 */


public class BenchMark {
    enum Cmd {TOUCH, MV, HEAD, LIST}
    public static Cmd cmd = Cmd.LIST;
    public static URI uri;
    public static String loglevel = "info";
    public static Boolean isRenameDir = true;
    public static Boolean show = false;
    public static String fileName = "BenchMark";
    public static int fileSize = 1024;
    public static int fileCount = 128;
    public static byte[] buf = null;

    private static Configuration config = new Configuration();
    private static FileSystem fs=null;
    private static Path src=null;
    private static Path dst=null;

    public static void displayUsage() {
        String usage = "Usage: benchmark <options>\n" +
                "Options:\n\t" +
                "[-cmd touch|mv|head|list]\n\t" +
                "[-mode <path>|<file>]\n\t" +
                "[-o show]\n\t" +
                "[-s <file size>]\n\t" +
                "[-c <file count>]\n\t" +
                "[-n <file name>]\n\t" +
                "-dfs hdfs://<ip or host>:<port>|ufile://<bucket>\n\t" +
                "<src path>\n\t" +
                "[<dst path>]";
        System.out.println(usage);
        System.exit(-1);
    }

    public static void parseInputs(String[] args) throws IOException, URISyntaxException {
        if (args.length == 0) {
            displayUsage();
        }
        for(int i = 0; i < args.length; ++i) {
 //           if (args[i].equals("-bucket")) {
 //               ++i;
 //               uri = new URI("ufile://"+args[i]);
 //               config.set("fs.AbstractFileSystem.ufile.impl", "cn.ucloud.ufile.fs.UFileFs");
 //               config.set("fs.ufile.impl", "cn.ucloud.ufile.fs.UFileFileSystem");
 //           } else if (args[i].equals("-endpoint")) {
 //               ++i;
 //               endpoint = args[i];
 //           } else if (args[i].equals("-accesskey")) {
 //               ++i;
 //               accesskey = args[i];
 //               config.set(Constants.CS_ACCESS_KEY, accesskey);
 //           } else if (args[i].equals("-secretkey")) {
 //               ++i;
 //               secretkey = args[i];
 //               config.set(Constants.CS_SECRET_KEY, secretkey);
 //           } else if (args[i].equals("-loglevel")) {
 //               ++i;
 //               loglevel = args[i];
 //               config.set(Constants.CS_LOG_LEVEL_KEY, loglevel);
            if (args[i].equals("-cmd")) {
                ++i;
                switch (args[i]) {
                    case "mv":
                        cmd = Cmd.MV;
                        break;
                    case "head":
                        cmd = Cmd.HEAD;
                        break;
                    case "list":
                        cmd = Cmd.LIST;
                        break;
                    case "touch":
                        cmd = Cmd.TOUCH;
                        break;
                    default:
                        displayUsage();
                }
            } else if (args[i].equals("-n")) {
                ++i;
                fileName = args[i];
            } else if (args[i].equals("-c")) {
                ++i;
                fileCount = Integer.parseInt(args[i]);
            } else if (args[i].equals("-s")) {
                ++i;
                if (fileSize > Integer.parseInt(args[i])) {
                    buf = new byte[Integer.parseInt(args[i])];
                } else {
                    buf = new byte[fileSize];
                }
                buf[0] = 'I';
                buf[1] = ' ';
                buf[2] = 'L';
                buf[3] = 'O';
                buf[4] = 'V';
                buf[5] = 'E';
                buf[6] = ' ';
                buf[7] = 'S';
                buf[8] = 'H';
                fileSize = Integer.parseInt(args[i]);
            } else if (args[i].equals("-o")) {
                ++i;
                switch (args[i]) {
                    case "show":
                        show = true;
                }
            } else if (args[i].equals("-mode")) {
                ++i;
                switch (args[i]) {
                    case "path":
                        break;
                    case "file":
                        isRenameDir = false;
                        break;
                    default:
                       displayUsage();
                }
            } else if (args[i].equals("-dfs")) {
                ++i;
                uri = new URI(args[i]);
            } else if ((args[i].equals("-help")) ||  (args[i].equals("-h"))) {
                displayUsage();
                System.exit(-1);
            } else {
                if (src == null) {
                    src = new Path(args[i]);
                } else if (dst == null) {
                    dst = new Path(args[i]);
                }
            }
        }

        switch (cmd) {
            case MV:
                if (src == null || dst == null ) displayUsage();
                break;
            case HEAD:
                if (src == null) displayUsage();
            case LIST:
                if (src == null) displayUsage();
                break;
            case TOUCH:
                if (src == null) displayUsage();
                break;
        }
    }

    public static void createFS() {
        try {
            fs = FileSystem.get(uri, config);
        } catch (IOException e ){
            e.printStackTrace();
        }
    }

    public static void touch() throws IOException {
        long begin = 0;
        begin = System.currentTimeMillis();
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] begin is %d", cmd.toString(),begin);
        for (int i = 0; i < fileCount; i++) {
            String file = String.format("%s/%s.%d", src.toString(), fileName, i);
            Path p = new Path(file);
            try {
                FileStatus fss = fs.getFileStatus(p);
            } catch (FileNotFoundException e) {
                try {
                    FSDataOutputStream out = fs.create(p);
                    int sum = 0;
                    do {
                        out.write(buf);
                        sum += buf.length;
                    } while(sum < fileSize);
                    out.close();
                } catch (Exception e1) {
                    UFileUtils.Error(UFileUtils.ParserLogLevel(loglevel), "[%s] %s exception", cmd.toString(), p.toString());
                    e1.printStackTrace();
                }
            }
        }
        long end = System.currentTimeMillis();
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] end is %d", cmd.toString(), end);
        double cost = (end - begin)/1000.0;
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] %d file create, taken %.3f seconds", cmd.toString(), fileCount, cost);
    }

    public static void listOrHead() throws IOException {
        FileStatus[] fsss = null;
        long begin = 0;
        try {
            if (cmd.equals(Cmd.LIST)) {
                begin = System.currentTimeMillis();
                UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] begin is %d", cmd.toString(),begin);
            }
            fsss  = fs.listStatus(src);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (cmd.equals(Cmd.HEAD)) {
            begin = System.currentTimeMillis();
            UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] begin is %d", cmd.toString(),begin);
            if (fsss != null)  {
                for (FileStatus fss: fsss ) {
                    if (show) {
                        String type;
                        if (fss.isDirectory()) {
                            type = "D";
                        } else {
                            type = "-";
                        }
                        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] %s %s %s %s %s",
                                cmd.toString(),
                                type,
                                fss.getPermission().toString(),
                                fss.getOwner(),
                                fss.getGroup(),
                                fss.getPath().toUri().toString());
                    }
                    fs.getFileStatus(fss.getPath());
                }
            }
        }
        long end = System.currentTimeMillis();
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] end is %d", cmd.toString(), end);
        double cost = (end - begin)/1000.0;
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[%s] %d file found, taken %.3f seconds", cmd.toString(), fsss.length, cost);
    }

    public static void rename() throws IOException {
        long begin;
        if (isRenameDir) {
            UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] directory");
            begin = System.currentTimeMillis();
            UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] begin is %d", begin);
            fs.rename(src, dst);
        } else {
            UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] files");
            FileStatus[] fsss = null;
            try {
                fsss  = fs.listStatus(src);
            } catch (Exception e) {
                e.printStackTrace();
            }

            begin = System.currentTimeMillis();
            UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] begin is %d", begin);
            if (fsss != null)  {
                long sum = 0;
                for (FileStatus fss: fsss ) {
                    String type;
                    if (!fss.isDirectory()) {
                        sum++;
                        fs.rename(fss.getPath(), dst);
                        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] %s to %s",
                                fss.getPath().toString(),
                                dst.toString());
                    }
                }
                UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] a total of %d files moved", sum);
            }
        }
        long end = System.currentTimeMillis();
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] end is %d", end);
        double cost = (end - begin)/1000.0;
        UFileUtils.Info(UFileUtils.ParserLogLevel(loglevel), "[rename] rename taken %.3f seconds", cost);
    }

    public static void main(String[] args) {
        try {
            parseInputs(args);
            createFS();
            switch (cmd) {
                case MV:
                    rename();
                    break;
                case HEAD:
                case LIST:
                    listOrHead();
                    break;
                case TOUCH:
                    touch();
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
