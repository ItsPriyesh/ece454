import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BackupConnectionPool {

    private static final int POOL_SIZE = 16;

    private final String host;
    private final int port;
    private final BlockingQueue<KeyValueService.Client> clients;

    public BackupConnectionPool(String host, int port) {
        this.host = host;
        this.port = port;
        clients = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            try {
                KeyValueService.Client client = createThriftClient(host, port);
                clients.put(client);
            } catch (Exception e) {
                System.out.println("Failed to insert client into pool");
            }
        }
    }

    public KeyValueService.Client obtainClient() throws InterruptedException {
        return clients.take();
    }

    public void releaseClient(KeyValueService.Client client) {
        // sus tings
        for (;;) {
            try {
                clients.put(client);
                break;
            } catch (InterruptedException e) { }
        }
    }

    public KeyValueService.Client recreateClientConnection() throws TTransportException {
        return createThriftClient(host, port);
    }

    private KeyValueService.Client createThriftClient(String host, int port) throws TTransportException {
        TSocket sock = new TSocket(host, port);
        TTransport transport = new TFramedTransport(sock);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }
}
