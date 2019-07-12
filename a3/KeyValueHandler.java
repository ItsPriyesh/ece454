import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    private BackupConnectionPool backupPool;

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
        if (isPrimary() && backupPool != null) {
            KeyValueService.Client client = null;
            try {
                client = backupPool.obtainClient();
                client.backupEntry(key, value, seq.incrementAndGet());
            } catch (Exception e) {
                client = backupPool.recreateClientConnection();
            } finally {
                if (client != null) {
                    backupPool.releaseClient(client);
                }
            }
        }
    }

    @Override
    public void backupEntry(String key, String value, int seq) throws TException {
        // Update the maps only if the key doesn't exist locally _or_
        // the received entry has a greater seq than the local entry
        int localSeq = seqMap.getOrDefault(key, -1);
        if (localSeq < 0 || seq >= localSeq) {
            seqMap.put(key, seq);
            localMap.put(key, value);
        }
    }

    @Override
    public void backupAll(List<String> keys, List<String> values) throws TException {
        for (int i = 0; i < keys.size(); i++) {
            if (!localMap.containsKey(keys.get(i))) {
                localMap.put(keys.get(i), values.get(i));
            }
        }
    }

    public boolean isPrimary() {
        return role == Role.PRIMARY;
    }

    public void onServersChanged(List<String> nodes) {
        role = determineRole(nodes);
        System.out.printf("set role of %s:%s to %s\n", host, port, role);

        if (nodes.size() > 1) {
            for (String node : nodes) {
                byte[] data;
                try {
                    data = curClient.getData().forPath(zkNode + "/" + node);
                } catch (Exception e) {
                    continue;
                }
                String string = new String(data);
                String[] host = string.split(":");
                int port = Integer.parseInt(host[1]);
                if (!host[0].equals(this.host) && port != this.port) {
                    backupPool = new BackupConnectionPool(host[0], port);
                    break;
                }
            }

            if (isPrimary() && backupPool != null) {
                // TODO Batch this request, if the map's too big thrift might fail
                KeyValueService.Client client = null;
                try {
                    client = backupPool.obtainClient();
                    List<String> keys = new ArrayList<>(localMap.keySet());
                    List<String> values = keys.stream().map(localMap::get).collect(Collectors.toList());
                    client.backupAll(keys, values);
                } catch (Exception e) {
                    // shiet
                } finally {
                    if (client != null) {
                        backupPool.releaseClient(client);
                    }
                }
            }
        }
    }

    private KeyValueHandler.Role determineRole(List<String> nodes) {
        if (isPrimary() || nodes.size() == 1) {
            return KeyValueHandler.Role.PRIMARY;
        } else {
            return KeyValueHandler.Role.BACKUP;
        }
    }
}
