package cn.ucloud.ufile.fs.common;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;

import cn.ucloud.ufile.fs.Configure;
import cn.ucloud.ufile.fs.UFileUtils;
import cn.ucloud.ufile.fs.zookeeperClient.ZookeeperClient;

public class VmdsAddressProvider {
    private String address;
    private ZookeeperClient zkClient;
    private Consumer<String> callback;
    private Configure cfg;
    public VmdsAddressProvider(Configure cfg, Consumer<String> callback) throws IOException{
        this.cfg = cfg;
        address = cfg.getMDSHost();
        this.callback = callback;
        if(address == null || address == ""){
            zkClient = new ZookeeperClient(new ZookeeperAddressProvider(cfg));
            int retryCount = 5;
            while(address == null || address == ""){
                retryCount--;
                if(retryCount == 0){
                    throw new IOException("no vmds address was registered under zookeeper's /us3vmds node, please check if any other process has registered an useless node beside");
                }
                String child = zkClient.getFirstChildWithParent("/us3vmds");
                if(child != null){
                    address = zkClient.get(child);
                    break;
                }
            }
        }
        callback.accept(address);
    }

    public void startWatching() throws Exception{
        if(zkClient!= null){
            zkClient.watch("/us3vmds", new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws IOException {
                    Type type = event.getType();
                    if ((type== Type.CHILD_ADDED) || type==Type.CHILD_REMOVED){
                        String child = zkClient.getFirstChildWithParent("/us3vmds");
                        if(child == null){
                            throw new IOException("no vmds online now");
                        }
                        String newAddress = zkClient.get(child);
                        if(!address.equals(newAddress)){
                            callback.accept(newAddress);
                            UFileUtils.Info(cfg.getLogLevel(), "[vmds failover] old: %s, new: %s", address, newAddress);
                            address = newAddress;
                        }
                    }
                }
            });
        }
    }

    public void stopWatching() throws IOException{
        if(zkClient!= null){
            zkClient.stopWatching();
        }
    }
}
