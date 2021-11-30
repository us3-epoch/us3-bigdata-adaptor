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
        public RetryInterceptor(LOGLEVEL loglevel) { this.loglevel = loglevel; }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            Response response = null;
            int tryCount = 0;
            int maxTry = request.method() == "GET"? Constants.GET_DEFAULT_MAX_TRYTIMES:Constants.DEFAULT_MAX_TRYTIMES;
            while(tryCount < maxTry) {
                response = chain.proceed(request);
                if (response.code()>= 500) {
                    try {
                        tryCount++;
                        Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                        
                        UFileUtils.Error(loglevel, " No.%d request method:%s url:%s retry interceptor trigger", tryCount, request.method(),
                                //adapted for sdk2.6.6
                                //request.httpUrl().toString());
                                request.url().toString());
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                } else {
                    break;
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


    public static class NetworkInterceptor implements Interceptor {
        private LOGLEVEL loglevel;
        public NetworkInterceptor(LOGLEVEL loglevel) { this.loglevel = loglevel; }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            Response response = null;
            IOException e = null;
            int tryCount = 0;
            while (tryCount < Constants.NETWORK_DEFAULT_MAX_TRYTIMES) {
                    try {
                        response = chain.proceed(request);
                    } catch (IOException e1) {
                        e = e1;
                        try {
                            tryCount++;
                            //adapted for sdk2.6.6
                            //UFileUtils.Error(loglevel, " No.%d request url:%s network interceptor trigger, %s", tryCount , request.httpUrl().toString(), e.toString());
                            UFileUtils.Error(loglevel, " No.%d request url:%s network interceptor trigger, %s", tryCount , request.url().toString(), e.toString());
                            Thread.sleep(tryCount * Constants.TRY_DELAY_BASE_TIME);
                        } catch (InterruptedException e2) {
                            e2.printStackTrace();
                        }
                        continue;
                    }

                    if (response != null) {
                        e = null;
                        if (tryCount > 0) {
                            //adapted for sdk2.6.6
                            //UFileUtils.Info(loglevel, " No.%d request url:%s network interceptor retry succ", tryCount, request.httpUrl().toString());
                            UFileUtils.Info(loglevel, " No.%d request url:%s network interceptor retry succ", tryCount, request.url().toString());
                        }
                        break;
                    }
            }

            if (e != null) { throw new IOException("network interceptor", e); }
            return response;
        }
    }

    public static void Install(LOGLEVEL loglevel) {
       UfileClient.Config ufileCfg = new UfileClient.Config();
       HttpClient.Config  httpCfg = ufileCfg.getHttpClientConfig();
       httpCfg.addInterceptor(new UserAgentInterceptor());
       httpCfg.addInterceptor(new RetryInterceptor(loglevel));
       httpCfg.addInterceptor(new NetworkInterceptor(loglevel));
       UfileClient.configure(ufileCfg);
    }
}
