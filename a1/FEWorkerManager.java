import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
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
            obtainClientConnection();
        }

        boolean isBusy() {
            return busy;
        }

        void obtainClientConnection() {
            transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
            client = new BcryptService.Client(new TBinaryProtocol(transport));
        }

        TTransport getTransport() {
            return transport;
        }

        BcryptService.Client getClient() {
            return client;
        }

        void setBusy(boolean busy) {
            this.busy = busy;
        }

        int getLoad() {
            return load;
        }

        void incrementLoad(int load) {
            this.load += load;
        }

        void decrementLoad(int load) {
            this.load -= load;
        }

        /**
         * BENode should send a heartbeat ping every 3 seconds, so we'll say its dead
         * if we haven't received anything in over 5 seconds
         */
        boolean hasPulse() {
            return System.currentTimeMillis() - lastHeartbeatTime < Duration.ofSeconds(5).toMillis();
        }

        @Override
        public String toString() {
            return String.format("Worker(@%s:%s; busy=%s, load=%s, lastHeartbeat=%s)",
                    host, port, busy, load,
                    new SimpleDateFormat("HH:mm:ss").format(new Date(lastHeartbeatTime))
            );
        }
    }

    private static final Map<String, WorkerMetadata> workers = new ConcurrentHashMap<>();

    static void addWorker(String host, String port) {
        final String key = host + port;
        final long timestamp = System.currentTimeMillis();
        // If we don't already know the worker, save it in our pool
        // Otherwise update the timestamp of the workers last heartbeat
        if (!workers.containsKey(key)) {
            workers.put(key, new WorkerMetadata(host, port, timestamp));
        } else {
            workers.get(key).lastHeartbeatTime = timestamp;
        }
        System.out.printf("Current worker pool size = (%s):\n", workers.size());
    }


    /**
     * Note: workers send a heart beat every 3 seconds to indicate they're alive.
     *       A worker returned by this method could potentially be down by the time it is accessed.
     */
    static synchronized WorkerMetadata findAvailableWorker() {
        // TODO : this is getting called by 2 threads on the FE node at once and they both return the same backend node
        if (workers.isEmpty()) return null;

        System.out.println("Finding available worker from : " + workers.values());

        WorkerMetadata worker;

        // Return the first worker that's not busy
        worker = workers.values()
                .stream()
                .filter(w -> !w.isBusy() && w.hasPulse())
                .findFirst()
                .orElse(null);

        if (worker != null) {
            worker.setBusy(true);
            return worker;
        }

        if (workers.isEmpty()) return null;

        // All workers are busy! Pick the one with the least load
        worker = workers.values()
                .stream()
                .min(Comparator.comparingInt(WorkerMetadata::getLoad))
                .orElse(null);

        if (worker != null) {
            // Re-create a new client connection to the BE (will spawn another thread on the least loaded BE)
            worker.obtainClientConnection();
        }

        return worker;
    }

    static void removeWorker(WorkerMetadata worker) {
        final String key = worker.host + worker.port;
        workers.remove(key);
    }

}
