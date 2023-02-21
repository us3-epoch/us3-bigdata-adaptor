package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.api.object.PutStreamApi;
import cn.ucloud.ufile.exception.UfileClientException;
import cn.ucloud.ufile.exception.UfileServerException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @Name: cn.ucloud.ufile.fs
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 2021/3/11 13:50
 */
public class MetadataService {
    public static ByteArrayInputStream emptyStream =new ByteArrayInputStream(Constants.empytBuf, 0, 0);

    public static void Nodify(UFileFileSystem fs, Map<String, String> userMeta,
                              String bucket, String key, String mimeType) throws IOException {
        int tryCount = 0;
        Exception exception = null;
        while (true) {
            try {
                PutStreamApi api = UfileClient.object(fs.getAuth(), fs.getMdsCfg())
                        //adapted for sdk2.6.6
                        //.putObject(emptyStream, mimeType)
                        .putObject(emptyStream, 0, mimeType)
                        .withMetaDatas(userMeta)
                        .nameAs(key)
                        .toBucket(bucket)
                        //adapted for sdk2.6.6
                        //.withVerifyMd5(false);
                        .withVerifyMd5(false, "");
                        api.execute();
                return;
            } catch (UfileClientException e) {
                e.printStackTrace();
                exception = e;
            } catch (UfileServerException e) {
                e.printStackTrace();
                exception = e;
            }
            try {
                // 3次无法覆盖vmds failover的时间，所以增加了重试次数
                if (tryCount < Constants.DEFAULT_MAX_TRYTIMES) {
                    Thread.sleep(tryCount*Constants.TRY_DELAY_BASE_TIME);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (tryCount >= Constants.DEFAULT_MAX_TRYTIMES) {
                break;
            }
            tryCount++;
        }
        throw UFileUtils.TranslateException("notify meta service", key, exception);
    }
}
