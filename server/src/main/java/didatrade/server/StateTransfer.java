package didatrade.server;

import didatrade.DidaTradePaxos.DecidedEntry;
import didatrade.DidaTradePaxos.StateTransferReply;
import didatrade.DidaTradePaxos.StateTransferRequest;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StateTransfer {
    private static final long RETRY_DELAY_MILLIS = 250;
    private static final long MAX_RETRY_DELAY_MILLIS = 2000;
    private static final ScheduledExecutorService RETRIES = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread worker = new Thread(task, "state-transfer-retry");
        worker.setDaemon(true);
        return worker;
    });

    // One latest prefix and one delivery state per learner, not a queue of snapshots.
    private StateTransferRequest prefix = StateTransferRequest.getDefaultInstance();
    private final Map<Integer, Delivery> deliveries = new HashMap<>();

    private static final class Delivery {
        int acknowledged;
        boolean active;
        long retryDelay = RETRY_DELAY_MILLIS;
    }

    private final DidaTradeServerState server;
    private final boolean              enabled;

    StateTransfer(DidaTradeServerState server, boolean enabled) {
        this.server  = server;
        this.enabled = enabled;
    }

    public void sendDecidedLog(int ballot) {
        if (!enabled) return;

        List<Integer> ready = new ArrayList<>();
        synchronized (this) {
            StateTransferRequest.Builder request = prefix.toBuilder()
                    .setBallot(ballot).setServerid(server.my_id);
            for (int position = prefix.getEntriesCount(); ; position++) {
                Integer reqid = server.paxos_log.getDecidedCommandId(position);
                RequestRecord record = reqid == null ? null : server.req_history.getIfExists(reqid);
                if (record == null || record.getRequest() == null) break;

                DidaTradeCommand command = record.getRequest();
                request.addEntries(DecidedEntry.newBuilder()
                        .setInstance(position)
                        .setBallot(server.paxos_log.getDecisionBallot(position))
                        .setReqid(reqid)
                        .setAction(command.getAction().ordinal())
                        .setUserid(command.getUserId())
                        .setQuantity(command.getQuantity())
                        .setStock(command.getStock()));
            }
            prefix = request.build();
            if (prefix.getEntriesCount() == 0) return;

            for (int replica : server.scheduler.learners(ballot)) {
                if (replica == server.my_id || server.channels[replica].isShutdown()) continue;
                Delivery delivery = deliveries.computeIfAbsent(replica, ignored -> new Delivery());
                if (!delivery.active && delivery.acknowledged < prefix.getEntriesCount()) {
                    delivery.active = true;
                    ready.add(replica);
                }
            }
        }
        for (int replica : ready) send(replica);
    }

    private void send(int replica) {
        StateTransferRequest request;
        synchronized (this) {
            if (server.channels[replica].isShutdown()) {
                deliveries.remove(replica);
                return;
            }
            request = prefix;
        }

        // Like Learn, keep one frozen handler instead of timing out and accumulating retries.
        Context.current().fork().run(() -> server.async_stubs[replica].withWaitForReady()
                .statetransfer(request, new StreamObserver<>() {
                    @Override public void onNext(StateTransferReply reply) { }

                    @Override public void onError(Throwable error) {
                        long delay;
                        synchronized (StateTransfer.this) {
                            if (server.channels[replica].isShutdown()) {
                                deliveries.remove(replica);
                                return;
                            }
                            Delivery delivery = deliveries.get(replica);
                            delay = delivery.retryDelay;
                            delivery.retryDelay = Math.min(delay * 2, MAX_RETRY_DELAY_MILLIS);
                        }
                        RETRIES.schedule(() -> send(replica), delay, TimeUnit.MILLISECONDS);
                    }

                    @Override public void onCompleted() {
                        boolean more;
                        synchronized (StateTransfer.this) {
                            Delivery delivery = deliveries.get(replica);
                            delivery.acknowledged = request.getEntriesCount();
                            delivery.retryDelay = RETRY_DELAY_MILLIS;
                            more = delivery.acknowledged < prefix.getEntriesCount();
                            delivery.active = more;
                        }
                        if (more) send(replica);
                    }
                }));
    }

    public int fillMissingEntries(StateTransferRequest request) {
        int filledCount = 0;

        for (DecidedEntry entry : request.getEntriesList()) {
            boolean hasCommand  = server.req_history.getIfExists(entry.getReqid()) != null;
            boolean hasDecision = server.paxos_log.isDecided(entry.getInstance());
            if (hasCommand && hasDecision) continue;

            if (!hasCommand) {
                DidaTradeCommand command = new DidaTradeCommand(
                        DidaTradeAction.values()[entry.getAction()],
                        entry.getUserid(), entry.getQuantity(), entry.getStock());
                server.req_history.addToPending(entry.getReqid(), new RequestRecord(entry.getReqid(), command));
            }
            if (!hasDecision) {
                server.paxos_log.decideEntry(entry.getInstance(), entry.getBallot(), entry.getReqid());
            }
            filledCount++;
        }

        if (filledCount > 0) {
            System.out.println("State transfer: filled " + filledCount + " entries from replica " + request.getServerid());
            server.updateCompletedBallot(request.getBallot());
            server.main_loop.wakeup();
        }
        return filledCount;
    }
}
