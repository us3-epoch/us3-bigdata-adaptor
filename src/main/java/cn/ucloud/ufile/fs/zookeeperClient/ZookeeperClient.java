package cn.ucloud.ufile.fs.zookeeperClient;

import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.ExponentialBackoffRetry;

import cn.ucloud.ufile.fs.common.ZookeeperAddressProvider;

public class ZookeeperClient {
    private CuratorFramework client;
    private PathChildrenCache pathChildrenCache;
    public ZookeeperClient(ZookeeperAddressProvider provider){
        this.client=CuratorFrameworkFactory.
        builder().connectString(provider.getConnectString()).
        sessionTimeoutMs(10000).retryPolicy(new
        ExponentialBackoffRetry(1000,3)).
        build();
        this.client.start();
    }
    public String get(String path)throws IOException{
        byte[] bytes = null;
        try {
            bytes = client.getData().forPath(path);
        } catch (Exception e) {
            throw new IOException("get vmds master address from zookeeper error: ", e);   
        }
        return new String(bytes);
    }

    public List<String> getChildren(String path) throws IOException{
        List<String> children = null;
        try {
            children = client.getChildren().forPath(path);
        } catch (Exception e) {
            throw new IOException("get vmds address list from zookeeper error: ", e);           
        }
        return children;
    }

    public String getFirstChildWithParent(String path) throws IOException{
        List<String> children = getChildren(path);
        Collections.sort(children);
        if(children.size()<1){
           return null;
        }
        return path + "/" + children.get(0);
    }

    public void watch(String path, PathChildrenCacheListener pathChildrenCacheListener ) throws Exception {
        pathChildrenCache = new PathChildrenCache(client,path,true);
        pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
    }

    public void stopWatching() throws IOException{
        pathChildrenCache.close();
    }
}
