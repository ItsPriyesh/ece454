import java.util.*;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.zookeeper.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());

        if (args.length != 4) {
            System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
            System.exit(-1);
        }

        CuratorFramework curClient =
                CuratorFrameworkFactory.builder()
                        .connectString(args[2])
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000)
                        .build();

        curClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                curClient.close();
            }
        });

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String zknode = args[3];

        KeyValueHandler handler = new KeyValueHandler(host, port, curClient, zknode);
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        TServerSocket socket = new TServerSocket(port);
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(64);
        TServer server = new TThreadPoolServer(sargs);
        log.info("Launching server");

        new Thread(new Runnable() {
            public void run() {
                server.serve();
            }
        }).start();

        // create an ephemeral node in ZooKeeper for this server
        String serverId = host + ":" + port;
        curClient.create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(zknode + "/server-", serverId.getBytes());
    }
}
