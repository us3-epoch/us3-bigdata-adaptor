package cn.ucloud.ufile.fs;

import cn.ucloud.ufile.UfileClient;
import cn.ucloud.ufile.http.HttpClient;
//adapted for sdk2.6.6
// import com.squareup.okhttp.Interceptor;
// import com.squareup.okhttp.Request;
// import com.squareup.okhttp.Response;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

/**
 * @Name: cn.ucloud.ufile.fs
 * @Description: 对所有失败请求(http状态码为5xx)进行重试
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 20:30
 */
public class UFileInterceptor{
    public static class RetryInterceptor implements Interceptor {
        private LOGLEVEL loglevel;
        private int retryTimes;
        public RetryInterceptor(LOGLEVEL loglevel, int retryTimes) {
            this.loglevel = loglevel;
            this.retryTimes = retryTimes;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            Response response = null;
            int tryCount = 0;
            int maxTry = retryTimes;
            while(tryCount < maxTry) {
                try {
                    response = chain.proceed(request);
                    if (response.code()>= 500) {
                        try {
                            Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                        } catch (InterruptedException e) {
                            UFileUtils.Error(loglevel, " No.%d request method:%s url:%s sleep interrupted, " +
                                            "can still retry for %d times", request.method(),
                                    request.url().toString(), maxTry - tryCount);
                            e.printStackTrace();
                        }
                    }else {
                        break;
                    }
                }catch (IOException e) {
                    e.printStackTrace();
                    UFileUtils.Error(loglevel, "request method:%s url:%s retry interceptor trigger, " +
                                    "can still retry for %d times", request.method(),
                            request.url().toString(), maxTry - tryCount);
                } finally {
                    tryCount++;
                }
            }
            return response;
        }
    }

    public static class UserAgentInterceptor implements Interceptor {
        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            Request newRequest = request.newBuilder().header("User-Agent",Constants.USER_AGENT + "/v" + Constants.VERSION).build();
            return chain.proceed(newRequest);
        }
    }

    public static void Install(LOGLEVEL loglevel, int timeout, int retryTimes) {
       UfileClient.Config ufileCfg = new UfileClient.Config();
       HttpClient.Config  httpCfg = ufileCfg.getHttpClientConfig();
       timeout = timeout * 1000;
       httpCfg.setTimeout(timeout, timeout, timeout);
       httpCfg.addInterceptor(new UserAgentInterceptor());
       httpCfg.addInterceptor(new RetryInterceptor(loglevel, retryTimes));
       UfileClient.configure(ufileCfg);
    }
}
