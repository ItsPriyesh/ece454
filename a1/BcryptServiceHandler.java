import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

    // # threads = # cores -> TODO: might want a few more threads than cores
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();

    private final boolean isBackEnd;

    BcryptServiceHandler() {
        this(false);
    }

    BcryptServiceHandler(boolean isFrontEnd) {
        this.isBackEnd = !isFrontEnd;
    }

    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
        return isBackEnd
                ? performHashPassword(password, logRounds)
                : offloadHashPasswordToBE(password, logRounds);
    }

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        return isBackEnd
                ? performCheckPassword(password, hash)
                : offloadCheckPasswordToBE(password, hash);
    }

    @Override
    public void ping(String host, String port) throws IllegalArgument, TException {
        // Front end received ping from backend @host:port
        System.out.println("Received ping from " + host + ":" + port);
        FEWorkerManager.addWorker(host, port);
    }

    private List<String> performHashPassword(List<String> passwords, short logRounds) throws IllegalArgument, TException {
        final int numPasswords = passwords.size();
        System.out.println("hashing passwords " + passwords.size());
        try {
            if (numPasswords > 1) { // parallelize the computation
                // Make sure this is <= number of fixed threads in the executor
                final int threads = Math.min(NUM_THREADS, numPasswords);
                final int passwordsPerThread = numPasswords / threads;
                System.out.printf("running in parallel: %s threads, %s pwds per thread\n", threads, passwordsPerThread);
                final String[] result = new String[numPasswords];
                ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

                for (int i = 0; i < threads; i++) {
                    final int start = i * passwordsPerThread;
                    final int end = i < threads - 1 ? (i + 1) * passwordsPerThread : numPasswords;
                    executorService.submit(() -> {
                        for (int j = start; j < end; j++) {
                            result[j] = BCrypt.hashpw(passwords.get(j), BCrypt.gensalt(logRounds));
                        }
                    });
                }

                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                System.out.println("finished hashing " + Arrays.asList(result));
                return Arrays.asList(result);
            } else { // run it on a single thread if there's only one password
                List<String> res = new ArrayList<>(passwords.size());
                for (String pass : passwords) {
                    res.add(BCrypt.hashpw(pass, BCrypt.gensalt(logRounds)));
                }
                System.out.println("finished hashing");
                return res;
            }
        } catch (Exception e) {
            System.out.println("hashing password failed! " + e.getMessage());
            throw new IllegalArgument(e.getMessage());
        }
    }

    private List<String> offloadHashPasswordToBE(List<String> passwords, short logRounds) throws IllegalArgument, TException {
        if (passwords.isEmpty()) {
            throw new IllegalArgument("Password list is empty!");
        } else if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("Log rounds out of range! Must be within [4, 30]");
        }

        FEWorkerManager.WorkerMetadata workerBE = FEWorkerManager.findAvailableWorker();
        System.out.println(Thread.currentThread().getName() + " FOUND WORKER: " + workerBE);
        while (workerBE != null) {
            // Found a worker! Sending workload to BE
            TTransport transportToBE = workerBE.getTransport();
            BcryptService.Client clientToBE = workerBE.getClient();
            try {
                final int workload = (int) (passwords.size() * Math.pow(2, logRounds));
                transportToBE.open();

                workerBE.setBusy(true);
                workerBE.incrementLoad(workload);
                List<String> hashes = clientToBE.hashPassword(passwords, logRounds);
                workerBE.decrementLoad(workload);
                workerBE.setBusy(false);
                System.out.println("Worker replied with hashes " + hashes);
                return hashes;
            } catch (Exception e) {
                // The chosen workerBE failed to hashPasswords (might be down)
                // remove it from our pool of workers and try to find another worker
                System.out.println("Worker failed: " + e.getMessage());
                FEWorkerManager.removeWorker(workerBE);
                workerBE = FEWorkerManager.findAvailableWorker();
            } finally {
                if (transportToBE.isOpen()) {
                    transportToBE.close();
                }
            }
        }

        // No workers available! Perform the hashing here on the FE
        System.out.println("No BE workers available! Performing locally");
        return performHashPassword(passwords, logRounds);
    }

    private List<Boolean> performCheckPassword(List<String> passwords, List<String> hashes) throws IllegalArgument, TException {
        final int numPasswords = passwords.size();
        System.out.println("checking passwords " + passwords.size());
        try {
            if (numPasswords > 1) { // parallelize the computation
                // Make sure this is <= number of fixed threads in the executor
                final int threads = Math.min(NUM_THREADS, numPasswords);
                final int passwordsPerThread = numPasswords / threads;
                final Boolean[] result = new Boolean[numPasswords];
                System.out.printf("running in parallel: %s threads, %s pwds per thread\n", threads, passwordsPerThread);
                ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

                for (int t = 0; t < threads; t++) {
                    final int start = t * passwordsPerThread;
                    final int end = t < threads - 1 ? (t + 1) * passwordsPerThread : numPasswords;
                    executorService.submit(() -> {
                        for (int i = start; i < end; i++) {
                            result[i] = BCrypt.checkpw(passwords.get(i), hashes.get(i));
                        }
                    });
                }

                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                System.out.println("finished checking " + Arrays.asList(result));
                return Arrays.asList(result);
            } else { // run it on a single thread if there's only one password
                List<Boolean> res = new ArrayList<>(passwords.size());
                for (int i = 0; i < passwords.size(); i++) {
                    res.add(BCrypt.checkpw(passwords.get(i), hashes.get(i)));
                }
                System.out.println("finished checking");
                return res;
            }
        } catch (Exception e) {
            System.out.println("hashing password failed!");
            throw new IllegalArgument(e.getMessage());
        }
    }

    private List<Boolean> offloadCheckPasswordToBE(List<String> password, List<String> hash) throws IllegalArgument, TException {
        // Error checking at FE (throw error at client):
        if (password.isEmpty() || hash.isEmpty()) {
            throw new IllegalArgument("Password or hash list is empty!");
        } else if (password.size() != hash.size()) {
            throw new IllegalArgument("Password and hash lists have unequal length!");
        }

        FEWorkerManager.WorkerMetadata workerBE = FEWorkerManager.findAvailableWorker();
        while (workerBE != null) {
            // Found a worker! Sending workload to BE
            TTransport transportToBE = workerBE.getTransport();
            BcryptService.Client clientToBE = workerBE.getClient();
            try {
                transportToBE.open();
                final int workload = password.size();

                workerBE.setBusy(true);
                workerBE.incrementLoad(workload);
                List<Boolean> checks = clientToBE.checkPassword(password, hash);
                workerBE.decrementLoad(workload);
                workerBE.setBusy(false);

                System.out.println("Worker replied with checks" + checks);
                return checks;
            } catch (Exception e) {
                // The chosen workerBE failed to hashPasswords (might be down)
                // remove it from our pool of workers and try to find another worker
                System.out.println("Worker failed: " + e.getMessage());
                FEWorkerManager.removeWorker(workerBE);
                workerBE = FEWorkerManager.findAvailableWorker();
            } finally {
                if (transportToBE.isOpen()) {
                    transportToBE.close();
                }
            }
        }

        // No workers available! Perform the hashing here on the FE
        return performCheckPassword(password, hash);
    }

}
