import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface {

    enum Role {
        PRIMARY, BACKUP,
    }

    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private Role role;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
    }

    @Override
    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public boolean isPrimary() {
        return role == Role.PRIMARY;
    }
}
