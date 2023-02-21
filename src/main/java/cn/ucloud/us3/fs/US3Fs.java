package cn.ucloud.us3.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Name: cn.ucloud.us3.fs
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 16:23
 */
public class US3Fs extends DelegateToFileSystem {
    public US3Fs(URI theUri, Configuration conf) throws IOException,
            URISyntaxException {
        super(theUri, new US3FileSystem(), conf, US3FileSystem.SCHEME,false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1;
    }
}

