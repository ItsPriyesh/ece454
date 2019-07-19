import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.TException;
import org.apache.zookeeper.WatchedEvent;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {

    private static int BACKUP_CHUNK_LENGTH = 300000;

    private final int port;
    private final String host;
    private final String zkNode;
    private final CuratorFramework curClient;

    private final AtomicInteger seq;
    private final Map<String, Integer> seqMap;
    private final Map<String, String> localMap;

    private boolean isPrimary;
    private BackupConnectionPool backupPool;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        localMap = new ConcurrentHashMap<>();
        seqMap = new ConcurrentHashMap<>();
        seq = new AtomicInteger(0);

        curClient.getChildren()
                .usingWatcher(this)
                .forPath(zkNode);
    }

    @Override
    public String get(String key) throws org.apache.thrift.TException {
        String ret = localMap.get(key);
        return ret == null ? "" : ret;
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        localMap.put(key, value);
        if (isPrimary && backupPool != null) {
            System.out.print("isPrimary and backup exists! forwarding operation to backup\n");
            KeyValueService.Client client = null;
            try {
                client = backupPool.obtainClient();
                client.backupEntry(key, value, seq.incrementAndGet());
            } catch (Exception e) {
                System.out.print("ERROR while forwarding to backup\n");
                e.printStackTrace();
                client = backupPool.recreateClientConnection();
            } finally {
                if (client != null) {
                    backupPool.releaseClient(client);
                } else {
                    backupPool = null;
                }
            }
        }
    }

    @Override
    public void backupEntry(String key, String value, int seq) throws TException {
        // Update the maps only if the key doesn't exist locally _or_
        // the received entry has a greater seq than the local entry
        System.out.print("backupEntry: received entry\n");
        int localSeq = seqMap.getOrDefault(key, -1);
        if (localSeq < 0 || seq >= localSeq) {
            seqMap.put(key, seq);
            localMap.put(key, value);
        }
    }

    @Override
    public void backupAll(List<String> keys, List<String> values) throws TException {
        System.out.printf("backupAll: received %s entries\n", keys.size());
        for (int i = 0; i < keys.size(); i++) {
            if (!localMap.containsKey(keys.get(i))) {
                localMap.put(keys.get(i), values.get(i));
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        List<String> nodes = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        isPrimary = determineRole(nodes);
        System.out.printf("process: set role of %s:%s to %s | nodes=%s, backupPool=%s\n", host, port, isPrimary ? "primary" : "backup", nodes.size(), backupPool);
        if (isPrimary) {
            if (nodes.size() > 1) {
                backupPool = obtainConnectionToBackup(nodes);
                if (backupPool != null) {
                    copyMapToBackup();
                }
            } else {
                backupPool = null;
            }
        }
    }

    private BackupConnectionPool obtainConnectionToBackup(List<String> zkServerNodes) {
        if (zkServerNodes.size() > 1) {
            for (String node : zkServerNodes) {
                System.out.println("Checking node: " + node);
                byte[] data;
                try {
                    data = curClient.getData().forPath(zkNode + "/" + node);
                } catch (Exception e) {
                    System.out.println("ERROR while finding nodes!");
                    e.printStackTrace();
                    continue;
                }
                String string = new String(data);
                String[] host = string.split(":");
                int port = Integer.parseInt(host[1]);
                System.out.println("data for node: " + string + " this.host/port = " + this.host + ":" + this.port);
                if (!(host[0].equals(this.host) && port == this.port)) {
                    System.out.println("Found backup, creating connection pool!");
                    return new BackupConnectionPool(host[0], port);
                }
            }
        }

        return null;
    }
    
    private void copyMapToBackup() {
        if (localMap.isEmpty()) return;

        if (isPrimary && backupPool != null) {

            List<String> keys = new ArrayList<>(localMap.keySet());
            List<String> values = keys.stream().map(localMap::get).collect(Collectors.toList());

            // See TFramedTransport.DEFAULT_MAX_LENGTH
            if (keys.size() <= BACKUP_CHUNK_LENGTH) {
                // Small enough to send in one frame
                invokeBackupAll(0, keys.size(), keys, values);
            } else {
                // Send in chunks
                int chunks = (int) Math.ceil(keys.size() / (double) BACKUP_CHUNK_LENGTH);
                System.out.println("Sending chunks " + chunks);
                for (int i = 0; i < chunks; i++) {
                    int start = i * BACKUP_CHUNK_LENGTH;
                    int end = i < chunks - 1 ? (i + 1) * BACKUP_CHUNK_LENGTH : keys.size();
                    System.out.println("sent chunk " + i + " from/to : " + start + " " + end);
                    invokeBackupAll(start, end, keys, values);
                }
            }
        }
    }

    private void invokeBackupAll(int start, int end, List<String> keys, List<String> values) {
        KeyValueService.Client client = null;
        try {
            client = backupPool.obtainClient();
            System.out.println("backupAll starting, sending " + (end - start));
            client.backupAll(keys.subList(start, end), values.subList(start, end));
            System.out.println("backupAll finished, sent " + (end - start));
        } catch (Exception e) {
            // shiet
            System.out.println("ERROR while copying entire map to backup!");
            e.printStackTrace();
        } finally {
            if (client != null) {
                backupPool.releaseClient(client);
            }
        }
    }

    /**
     * true => primary, false => backup
     */
    private boolean determineRole(List<String> nodes) {
        return isPrimary || nodes.size() == 1;
    }
}