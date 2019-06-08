import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

    private final boolean isBackEnd;

    public BcryptServiceHandler() {
        this(false);
    }

    public BcryptServiceHandler(boolean isFrontEnd) {
        isBackEnd = !isFrontEnd;
    }

    private static List<String> performHashPassword(List<String> passwords, short logRounds) {
        List<String> res = new ArrayList<>(passwords.size());
        for (String pass : passwords) {
            res.add(BCrypt.hashpw(pass, BCrypt.gensalt(logRounds)));
        }
        return res;
    }

    private static List<Boolean> performCheckPassword(List<String> passwords, List<String> hashes) {
        List<Boolean> res = new ArrayList<>(passwords.size());
        for (int i = 0; i < passwords.size(); i++) {
            res.add(BCrypt.checkpw(passwords.get(i), hashes.get(i)));
        }
        return res;
    }


    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        if (password.isEmpty()) {
            throw new IllegalArgument("Password list is empty!");
        } else if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("Log rounds out of range! Must be within [4, 30]");
        }

        if (isBackEnd) {
            // Backend Worker node:
            // - Update timestamp of last received batch (to know if FE is still alive)
            // - Execute hashing:
            //   - (multithread using a thread pool with size # threads = # cores)
            //   - Split up batch and distribute the list of passwords amongst the threads
            //  - if passwords.size == 1, just run hashing sequentially
            // - Return result

            try {

            } catch (Exception e) {
                throw new IllegalArgument(e.getMessage());
            }
            return Collections.emptyList();

        } else {
            // Front-end node: must try to find a backend node to send workload to
            // - Try to find an available backend node
            //   - Establish a BcryptService.Client client connection to the chosen backend node
            //   - Mark node as occupied and set its current load
            //   - Run backendClient.hashPasswords()
            //   - Mark node as available and reduce its current load
            //   - Return result
            // - If no available backends, run bcryptHashPasswords here on the FE
            //   - Return result
            FEWorkerManager.WorkerMetadata workerBE = FEWorkerManager.findAvailableWorker();
            TTransport transportToBE = workerBE.getTransport();
            BcryptService.Client clientToBE = workerBE.getClient();
            try {
                transportToBE.open();
                // TODO: mark worker as occupied and set its load
                List<String> hashes = clientToBE.hashPassword(password, logRounds);
                // TODO: mark worker as unoccupied and decrease its load
                return hashes;
            } catch (Exception e) {

            } finally {
                if (transportToBE.isOpen()) {
                    transportToBE.close();
                }
            }

            return Collections.emptyList();
        }

    }

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        if (password.isEmpty() || hash.isEmpty()) {
            throw new IllegalArgument("Password or hash list is empty!");
        } else if (password.size() != hash.size()) {
            throw new IllegalArgument("Password and hash lists have unequal length!");
        }
        if (isBackEnd) {

        } else {

        }
    }

    @Override
    public void ping(String host, String port) throws IllegalArgument, TException {
        // Front end received ping from backend @host:port
        FEWorkerManager.addWorker(host, port);
    }
}
