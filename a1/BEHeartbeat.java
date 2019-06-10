import org.apache.thrift.transport.TTransport;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BEHeartbeat {

    private static final int PING_PERIOD_SECONDS = 3;

    private final Runnable ping;
    private final ScheduledExecutorService pingExecutor = Executors.newSingleThreadScheduledExecutor();

    BEHeartbeat(String host, String port, TTransport transport, BcryptService.Client client) {
        ping = () -> {
            System.out.println("Attempting to ping FE");
            try {
                transport.open();
                client.ping(host, port);
                transport.close();
                System.out.println("Ping FE successful");
            } catch (Exception e) {
                System.out.println("FE not up yet");
            }
        };
    }

    void start() {
        pingExecutor.scheduleAtFixedRate(ping, 0, PING_PERIOD_SECONDS, TimeUnit.SECONDS);
    }
}
