package didatrade.server;

import didatrade.DidaTradeMain;
import didatrade.DidaTradeMainServiceGrpc;
import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import junit.framework.TestCase;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Real gRPC processes; directed Learn messages exercise learner/executor ordering separately. */
public class ConsensusRegressionTest extends TestCase {
    public void testClassicPaxos() throws Exception {
        checkMode("classic");
    }

    public void testMultiPaxos() throws Exception {
        checkMode("multi");
    }

    public void testClassicPhaseTwoTimeoutKeepsTheSameProposal() throws Exception {
        try (Cluster cluster = new Cluster("classic-retry", true)) {
            Future<Boolean> first = cluster.submit(() -> cluster.add(0, 101, 42, 20));
            cluster.await(() -> cluster.delayedAcceptors.stream().allMatch(peer -> peer.values.size() == 1));
            Future<Boolean> second = cluster.submit(() -> cluster.add(0, 201, 43, 20));
            assertTrue(first.get(12, TimeUnit.SECONDS));
            assertTrue(second.get(12, TimeUnit.SECONDS));
            for (DelayedAcceptor peer : cluster.delayedAcceptors) {
                assertTrue(peer.values.size() >= 2);
                assertEquals(101, peer.values.get(0).intValue());
                assertEquals(101, peer.values.get(1).intValue());
            }
        }
    }

    private void checkMode(String mode) throws Exception {
        try (Cluster cluster = new Cluster(mode)) {
            // The leader must retry an expired attempt without another client wakeup.
            cluster.debug(1, DebugMode.FREEZE);
            cluster.debug(2, DebugMode.FREEZE);
            Future<Boolean> first = cluster.submit(() -> cluster.add(0, 101, 42, 20));
            Future<Boolean> duplicate = cluster.submit(() -> cluster.add(0, 101, 42, 20));
            cluster.await(() -> Files.readString(cluster.logs.get(0))
                    .split("Going to send phase 1", -1).length >= 3);
            assertFalse(first.isDone());
            assertFalse(duplicate.isDone());
            cluster.debug(1, DebugMode.UNFREEZE);
            assertTrue(first.get(12, TimeUnit.SECONDS));
            assertTrue(duplicate.get(12, TimeUnit.SECONDS));
            cluster.debug(2, DebugMode.UNFREEZE);
            cluster.addEverywhere(101, 42, 20);

            cluster.addEverywhere(201, 43, 20);
            for (int replica = 0; replica < 3; replica++) {
                assertTrue(cluster.main(replica).sell(DidaTradeMain.SellRequest.newBuilder()
                        .setReqid(301).setUserid(42).setQuantity(50).build()).getResult());
                assertTrue(cluster.main(replica).buy(DidaTradeMain.BuyRequest.newBuilder()
                        .setReqid(401).setUserid(43).setQuantity(50).build()).getResult());
                assertEquals(30, cluster.balance(replica, 501, 42));
            }
            // Step 4 retransmissions are fresh local reads, not cached log commands.
            for (int replica = 0; replica < 3; replica++) {
                assertTrue(cluster.main(replica).buy(DidaTradeMain.BuyRequest.newBuilder()
                        .setReqid(601).setUserid(42).setQuantity(50).build()).getResult());
                assertEquals(15, cluster.balance(replica, 501, 42));
                assertEquals(15, cluster.balance(replica, 701, 42));
            }

            List<Future<Boolean>> concurrent = new ArrayList<>();
            for (int user = 50; user < 54; user++) {
                final int uid = user;
                for (int replica = 0; replica < 3; replica++) {
                    final int target = replica;
                    concurrent.add(cluster.submit(() -> cluster.add(target, 1000 + uid, uid, 40)));
                }
            }
            for (Future<Boolean> reply : concurrent) assertTrue(reply.get(12, TimeUnit.SECONDS));

            // Schedule A still progresses with quorum 2 and a frozen previous leader.
            cluster.debug(0, DebugMode.FREEZE);
            Future<Integer> changed = cluster.submit(() -> cluster.master(1).newballot(
                    DidaTradeMaster.NewBallotRequest.newBuilder().setReqid(2000)
                            .setNewballot(1).setCompletedballot(-1).build()).getCompletedballot());
            cluster.await(() -> cluster.paxos(1).phaseone(DidaTradePaxos.PhaseOneRequest.newBuilder()
                    .setRequestballot(0).build()).getMaxballot() >= 1);
            assertTrue(cluster.add(1, 2001, 60, 25));
            assertTrue(cluster.add(2, 2001, 60, 25));
            assertTrue(changed.get(12, TimeUnit.SECONDS) >= 1);
            cluster.debug(0, DebugMode.UNFREEZE);
            assertTrue(cluster.add(0, 2001, 60, 25));
            // Deliver to the current leader first when these broadcasts are sequential.
            for (int replica : new int[] {1, 0, 2}) {
                assertEquals(25, cluster.balance(replica, 2002, 60));
                assertTrue(cluster.main(replica).populate(DidaTradeMain.PopulateRequest.newBuilder()
                        .setReqid(2003).setQuantity(12).build()).getResult());
                assertEquals(12, cluster.balance(replica, 2004, 11));
            }
            cluster.checkExecutedPrefixesMatch();
            cluster.checkStatesMatch();

            // Followers must wait at a hole, then consume a repeated ID without executing it.
            int next = cluster.paxos(2).phaseone(DidaTradePaxos.PhaseOneRequest.newBuilder()
                    .setRequestballot(1).build()).getAcceptedEntriesList().stream()
                    .mapToInt(DidaTradePaxos.AcceptedEntry::getInstance).max().orElseThrow() + 1;
            cluster.decideOnFollower(next + 1, 3001);
            Future<Boolean> later = cluster.submit(() -> cluster.add(2, 3001, 80, 17));
            Thread.sleep(200);
            assertFalse("Must wait for the earlier position", later.isDone());
            cluster.decideOnFollower(next, 601);
            assertTrue(later.get(5, TimeUnit.SECONDS));

            // Decision before payload: preserve the later-write ordering check, and
            // make the local read explicitly depend on both first executions.
            long before = cluster.balanceReply(2, 3000, 42, 0).getAppliedVersion();
            cluster.decideOnFollower(next + 2, 3002);
            cluster.decideOnFollower(next + 3, 3003);
            Future<Boolean> laterWrite = cluster.submit(() -> cluster.add(2, 3003, 82, 21));
            Future<Integer> balance = cluster.submit(() ->
                    cluster.balanceReply(2, 3004, 81, before + 2).getBalance());
            Thread.sleep(200);
            assertFalse("Later write must wait for the earlier payload", laterWrite.isDone());
            assertFalse("Must wait for the earlier payload", balance.isDone());
            assertTrue(cluster.add(2, 3002, 81, 19));
            assertTrue(laterWrite.get(5, TimeUnit.SECONDS));
            assertEquals(19, balance.get(5, TimeUnit.SECONDS).intValue());
            cluster.decideOnFollower(next + 4, 601); // Another duplicate log position.
            DidaTradeMain.BalanceReply finalRead = cluster.balanceReply(2, 3005, 42, before + 2);
            assertEquals(15, finalRead.getBalance());
            assertEquals("Only the two fresh writes advance the version", before + 2,
                    finalRead.getAppliedVersion());
        }
    }

    private static final class Cluster implements AutoCloseable {
        private final List<Process> servers = new ArrayList<>();
        private final List<io.grpc.Server> peers = new ArrayList<>();
        private final List<DelayedAcceptor> delayedAcceptors = new ArrayList<>();
        private final List<ManagedChannel> channels = new ArrayList<>();
        private final List<Path> logs = new ArrayList<>();
        private final java.util.concurrent.ExecutorService pool = Executors.newFixedThreadPool(12);

        Cluster(String mode) throws Exception {
            this(mode, false);
        }

        Cluster(String mode, boolean delayPhaseTwo) throws Exception {
            int base = freeConsecutivePorts();
            Path directory = Path.of("target", "consensus-regression", mode);
            Files.createDirectories(directory);
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            try {
                for (int replica = 0; replica < 3; replica++) {
                    Path log = directory.resolve("s" + replica + ".log");
                    logs.add(log);
                    if (delayPhaseTwo && replica > 0) {
                        DelayedAcceptor peer = new DelayedAcceptor(replica);
                        delayedAcceptors.add(peer);
                        peers.add(io.grpc.ServerBuilder.forPort(base + replica).addService(peer).build().start());
                    } else {
                        servers.add(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-cp", classpath, DidaTradeServer.class.getName(), Integer.toString(base),
                                Integer.toString(replica), "A", delayPhaseTwo ? "classic" : mode)
                                .redirectErrorStream(true).redirectOutput(log.toFile()).start());
                    }
                    channels.add(ManagedChannelBuilder.forAddress("localhost", base + replica).usePlaintext().build());
                }
                for (int replica = 0; replica < 3; replica++) {
                    final int port = base + replica;
                    await(() -> {
                        try (Socket ignored = new Socket("localhost", port)) {
                            return true;
                        } catch (java.io.IOException notReady) {
                            return false;
                        }
                    });
                }
            } catch (Exception | AssertionError error) {
                close();
                throw error;
            }
        }

        private static int freeConsecutivePorts() throws Exception {
            for (int attempt = 0; attempt < 100; attempt++) {
                try (ServerSocket first = new ServerSocket(0)) {
                    int base = first.getLocalPort();
                    if (base > 65533) continue;
                    try (ServerSocket second = new ServerSocket(base + 1);
                         ServerSocket third = new ServerSocket(base + 2)) {
                        return base;
                    } catch (java.io.IOException occupied) {
                        // Retry a different range.
                    }
                }
            }
            throw new IllegalStateException("No free range for three replicas");
        }

        void await(Callable<Boolean> condition) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            while (System.nanoTime() < until) {
                for (Process server : servers) assertTrue("Replica exited; inspect " + logs, server.isAlive());
                if (condition.call()) return;
                Thread.sleep(50);
            }
            fail("Timed out; inspect " + logs);
        }

        <T> Future<T> submit(Callable<T> action) {
            return pool.submit(action);
        }

        DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub main(int replica) {
            return DidaTradeMainServiceGrpc.newBlockingStub(channels.get(replica)).withDeadlineAfter(12, TimeUnit.SECONDS);
        }

        DidaTradeMasterServiceGrpc.DidaTradeMasterServiceBlockingStub master(int replica) {
            return DidaTradeMasterServiceGrpc.newBlockingStub(channels.get(replica)).withDeadlineAfter(12, TimeUnit.SECONDS);
        }

        DidaTradePaxosServiceGrpc.DidaTradePaxosServiceBlockingStub paxos(int replica) {
            return DidaTradePaxosServiceGrpc.newBlockingStub(channels.get(replica)).withDeadlineAfter(5, TimeUnit.SECONDS);
        }

        void debug(int replica, DebugMode mode) {
            master(replica).setdebug(DidaTradeMaster.SetDebugRequest.newBuilder()
                    .setReqid(9000 + replica).setMode(mode.getCode()).build());
        }

        boolean add(int replica, int id, int user, int cash) {
            return main(replica).adduser(DidaTradeMain.AddUserRequest.newBuilder()
                    .setReqid(id).setUserid(user).setCash(cash).setStock(20).build()).getResult();
        }

        void addEverywhere(int id, int user, int cash) {
            for (int replica = 0; replica < 3; replica++) assertTrue(add(replica, id, user, cash));
        }

        int balance(int replica, int id, int user) {
            return balanceReply(replica, id, user, 0).getBalance();
        }

        DidaTradeMain.BalanceReply balanceReply(int replica, int id, int user, long minVersion) {
            DidaTradeMain.BalanceReply result = main(replica).balance(DidaTradeMain.BalanceRequest.newBuilder()
                    .setReqid(id).setUserid(user).setMinVersion(minVersion).build());
            assertTrue(result.getResult());
            assertTrue(result.getAppliedVersion() >= minVersion);
            return result;
        }

        void decideOnFollower(int instance, int id) {
            for (int acceptor = 0; acceptor < 2; acceptor++) {
                paxos(2).learn(DidaTradePaxos.LearnRequest.newBuilder().setInstance(instance)
                        .setBallot(1).setValue(id).setServerid(acceptor).build());
            }
        }

        void checkExecutedPrefixesMatch() throws Exception {
            List<String> expected = executions(logs.get(0));
            assertFalse(expected.isEmpty());
            for (int replica = 1; replica < 3; replica++) assertEquals(expected, executions(logs.get(replica)));
        }

        List<String> executions(Path log) throws Exception {
            return Files.readAllLines(log).stream()
                    .filter(line -> line.startsWith("Setting response for command with id = ")).toList();
        }

        void checkStatesMatch() throws Exception {
            for (int replica : new int[] {1, 0, 2}) {
                assertTrue(main(replica).dump(DidaTradeMain.DumpRequest.newBuilder()
                        .setReqid(2005).build()).getResult());
            }
            String expected = dump(logs.get(0));
            for (int replica = 1; replica < 3; replica++) assertEquals(expected, dump(logs.get(replica)));
        }

        String dump(Path log) throws Exception {
            String contents = Files.readString(log);
            int begin = contents.lastIndexOf(" ----------- Users ----------- ");
            assertTrue(begin >= 0);
            int end = contents.indexOf(" -----------    done     ----------- ", begin);
            assertTrue(end > begin);
            return contents.substring(begin, end);
        }

        @Override
        public void close() throws Exception {
            pool.shutdownNow();
            for (ManagedChannel channel : channels) channel.shutdownNow();
            for (io.grpc.Server peer : peers) peer.shutdownNow();
            for (Process server : servers) server.destroy();
            for (Process server : servers) {
                if (!server.waitFor(3, TimeUnit.SECONDS)) {
                    server.destroyForcibly();
                    assertTrue(server.waitFor(3, TimeUnit.SECONDS));
                }
            }
            for (io.grpc.Server peer : peers) assertTrue(peer.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /** First Phase 2 stalls before voting; retries reply normally. No fast protocol involved. */
    private static final class DelayedAcceptor extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
        private final int id;
        private final List<Integer> values = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final java.util.Map<Integer, DidaTradePaxos.AcceptedEntry> accepted = new java.util.concurrent.ConcurrentHashMap<>();

        DelayedAcceptor(int id) {
            this.id = id;
        }

        @Override
        public void phaseone(DidaTradePaxos.PhaseOneRequest request,
                             io.grpc.stub.StreamObserver<DidaTradePaxos.PhaseOneReply> observer) {
            observer.onNext(DidaTradePaxos.PhaseOneReply.newBuilder().setServerid(id)
                    .setInstance(request.getInstance()).setRequestballot(request.getRequestballot())
                    .setAccepted(true).setValballot(-1).addAllAcceptedEntries(accepted.values()).build());
            observer.onCompleted();
        }

        @Override
        public void phasetwo(DidaTradePaxos.PhaseTwoRequest request,
                             io.grpc.stub.StreamObserver<DidaTradePaxos.PhaseTwoReply> observer) {
            if (request.getInstance() == 0) {
                values.add(request.getValue());
                if (values.size() == 1) return;
            }
            accepted.put(request.getInstance(), DidaTradePaxos.AcceptedEntry.newBuilder()
                    .setInstance(request.getInstance()).setValballot(request.getRequestballot())
                    .setValue(request.getValue()).build());
            observer.onNext(DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(id)
                    .setInstance(request.getInstance()).setRequestballot(request.getRequestballot())
                    .setAccepted(true).build());
            observer.onCompleted();
        }
    }
}
