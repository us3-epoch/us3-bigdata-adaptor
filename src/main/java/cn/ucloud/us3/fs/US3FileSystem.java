package cn.ucloud.us3.fs;

import cn.ucloud.ufile.fs.UFileFileSystem;

/**
 * @Name: cn.ucloud.us3.fs
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 16:20
 */
public class US3FileSystem extends UFileFileSystem {
    public static String SCHEME = "us3";

    public US3FileSystem() {
        super();
        super.SCHEME = SCHEME;
    }
}
