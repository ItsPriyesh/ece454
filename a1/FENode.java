import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;


public class FENode {
    static Logger log;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java FENode FE_port");
            System.exit(-1);
        }

        // initialize log4j
        BasicConfigurator.configure();
        log = Logger.getLogger(FENode.class.getName());

        int portFE = Integer.parseInt(args[0]);
        log.info("Launching FE node on port " + portFE);

        // launch Thrift server
        BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler(true));
        TNonblockingServerTransport socket = new TNonblockingServerSocket(portFE);
        THsHaServer.Args sargs = new THsHaServer.Args(socket)
                .protocolFactory(new TBinaryProtocol.Factory())
                .transportFactory(new TFramedTransport.Factory())
                .processorFactory(new TProcessorFactory(processor));
//                .maxWorkerThreads(22);
        THsHaServer server = new THsHaServer(sargs);
        server.serve();
    }

    // TODO Server types?
    // FE should probably be
    // BE should probably be a thread pool server
}
