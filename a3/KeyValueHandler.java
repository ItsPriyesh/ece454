import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.*;
import org.apache.thrift.TException;


public class KeyValueHandler implements KeyValueService.Iface {

    enum Role {
        PRIMARY, BACKUP,
    }

    private final int port;
    private final String host;
    private final String zkNode;
    private final CuratorFramework curClient;

    private final AtomicInteger seq;
    private final Map<String, Integer> seqMap;
    private final Map<String, String> localMap;

    private Role role;
    private boolean backupExists;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        localMap = new ConcurrentHashMap<>();
        seqMap = new ConcurrentHashMap<>();
        seq = new AtomicInteger();
    }

    @Override
    public String get(String key) throws org.apache.thrift.TException {
        String ret = localMap.get(key);
        return ret == null ? "" : ret;
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        localMap.put(key, value);
        if (isPrimary() && backupExists) {
            // TODO: somehow obtain a client connection to the backup node
            KeyValueService.Client backupClient = null;
            backupClient.copyPut(key, value, seq.incrementAndGet());
        }
    }

    @Override
    public void copyPut(String key, String value, int seq) throws TException {
        // Update the maps only if the key doesn't exist locally _or_
        // the received entry has a greater seq than the local entry
        int localSeq = seqMap.getOrDefault(key, -1);
        if (localSeq < 0 || seq >= localSeq) {
            seqMap.put(key, seq);
            localMap.put(key, value);
        }
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public boolean isPrimary() {
        return role == Role.PRIMARY;
    }
}
