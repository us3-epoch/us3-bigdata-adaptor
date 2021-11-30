package cn.ucloud.ufile.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UFileFs extends DelegateToFileSystem {
    public UFileFs(URI theUri, Configuration conf) throws IOException,
            URISyntaxException {
        super(theUri, new UFileFileSystem(), conf, UFileFileSystem.SCHEME,false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1;
    }
}
