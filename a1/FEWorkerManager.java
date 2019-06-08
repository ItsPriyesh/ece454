import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.ConcurrentHashMap;

public class FEWorkerManager {

    static class WorkerMetadata {
        private final String host, port;
        private final TTransport transport;
        private final BcryptService.Client client;

        private boolean occupied;
        private int load;

        WorkerMetadata(String host, String port) {
            this.host = host;
            this.port = port;

            transport = new TFramedTransport(new TSocket(host, Integer.parseInt(port)));
            client = new BcryptService.Client(new TBinaryProtocol(transport));
        }

        public TTransport getTransport() {
            return transport;
        }

        public BcryptService.Client getClient() {
            return client;
        }
    }

    private static final ConcurrentHashMap<String, WorkerMetadata> workers = new ConcurrentHashMap<>();

    static void addWorker(String host, String port) {
        final String workerId = host + port;
        workers.put(workerId, new WorkerMetadata(host, port));
    }

    // return null only if all back-ends are down
    static WorkerMetadata findAvailableWorker() {
        if (workers.isEmpty()) return null;

        // load balancing
        // round robin
        // ???
        return null;
    }

    // Call this when we havent received a ping from a BENode in a while
    static void removeWorker() {

    }
}
