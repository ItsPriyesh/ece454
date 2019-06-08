import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class BENode {
    static Logger log;

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: java BENode FE_host FE_port BE_port");
            System.exit(-1);
        }

        String hostFE = args[0];
        int portFE = Integer.parseInt(args[1]);

        // initialize log4j
        BasicConfigurator.configure();
        log = Logger.getLogger(BENode.class.getName());

        // create client to front end
        TSocket sock = new TSocket(hostFE, portFE);
        TTransport transport = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(transport);
        BcryptService.Client feClient = new BcryptService.Client(protocol);


        // Continuously ping the front-end to notify of this backend nodes existence
        BEHeartbeat heartBeat = new BEHeartbeat(getHostName(), args[2], transport, feClient);
        heartBeat.start();

        // launch Thrift server
        int portBE = Integer.parseInt(args[2]);
        log.info("Launching BE node on port " + portBE + " at host " + getHostName());
        BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
        TServerSocket socket = new TServerSocket(portBE);
        TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        //sargs.maxWorkerThreads(64);
        TSimpleServer server = new TSimpleServer(sargs);
        server.serve();
    }

    private static void print(String s) {
        System.out.println(Thread.currentThread().getName() + ": " + s);
    }

    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }
}
