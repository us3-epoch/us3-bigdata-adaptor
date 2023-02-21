package cn.ucloud.ufile.fs.common;

import java.io.IOException;

import cn.ucloud.ufile.fs.Configure;

// provider会按照如下优先级搜索zookeeper的地址: 在core-site.xml中直接配置的->去zookeeper中拿
public class ZookeeperAddressProvider {
    private String connectString;
    public ZookeeperAddressProvider(Configure cfg) throws IOException{
        connectString = cfg.getCustomZookeeperAddresses();
        if(connectString != null && connectString !=""){
            return;
        }
        connectString = cfg.getHdZookeeperAddresses();
        if(connectString != null && connectString !=""){
            return;
        }
        throw new IOException("cannot find vmds address in current environment");
    }

    public String getConnectString(){
        return connectString;
    }
}
