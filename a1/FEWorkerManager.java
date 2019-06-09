import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FEWorkerManager {

    static class WorkerMetadata {
        private final String host, port;
        private TTransport transport;
        private BcryptService.Client client;

        private boolean busy;
        private int load;
        private long lastHeartbeatTime;

        WorkerMetadata(String host, String port, long time) {
            this.host = host;
            this.port = port;
            this.lastHeartbeatTime = time;
            buildClientConnection();
        }

        public boolean isBusy() {
            return busy;
        }

        public void buildClientConnection() {
            transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
            client = new BcryptService.Client(new TBinaryProtocol(transport));
        }

        public TTransport getTransport() {
            return transport;
        }

        public BcryptService.Client getClient() {
            return client;
        }

        public void setBusy(boolean busy) {
            this.busy = busy;
        }

        public int getLoad() {
            return load;
        }

        public void incrementLoad(int load) {
            this.load += load;
        }

        public void decrementLoad(int load) {
            this.load -= load;
        }

        /**
         * BENode should send a heartbeat ping every 3 seconds, so we'll say its dead
         * if we haven't received anything in over 5 seconds
         */
        public boolean hasPulse() {
            return System.currentTimeMillis() - lastHeartbeatTime < Duration.ofSeconds(5).toMillis();
        }

        @Override
        public String toString() {
            return String.format("@%s:%s; busy=%s, load=%s, lastHeartbeat=%s",
                    host, port, busy, load, lastHeartbeatTime);
        }
    }

    private static final Map<String, WorkerMetadata> workers = new ConcurrentHashMap<>();

    static void addWorker(String host, String port) {
        final String key = host + port;
        final long timestamp = System.currentTimeMillis();
        if (!workers.containsKey(key)) {
            workers.put(key, new WorkerMetadata(host, port, timestamp));
        } else {
            workers.get(key).lastHeartbeatTime = timestamp;
        }
    }


    /**
     * Note: workers send a heart beat every 3 seconds to indicate they're alive.
     *       A worker returned by this method could potentially be down by the time it is accessed.
     */
    static WorkerMetadata findAvailableWorker() {
        if (workers.isEmpty()) return null;

        System.out.println("Finding available worker from : " + workers);

        // Return the first worker that's not busy
        for (WorkerMetadata worker : workers.values()) {
            if (!worker.isBusy() && worker.hasPulse()) {
                return worker;
            }
        }

        if (workers.isEmpty()) return null;

        // All workers are busy! Pick the one with the least load
        WorkerMetadata worker = Collections.min(workers.values(),
                Comparator.comparingInt(WorkerMetadata::getLoad));

        // Re-create a new client connection to the BE (will spawn another thread on the least loaded BE)
        worker.buildClientConnection();

        return worker;
    }

    // Call this when we havent received a ping from a BENode in a while
    static void removeWorker(WorkerMetadata worker) {
        final String key = worker.host + worker.port;
        workers.remove(key);
    }

    static boolean hasWorker(String host, String port) {
        final String key = host + port;
        return workers.containsKey(key);
    }
}
