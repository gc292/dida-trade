package didatrade.server;

import didatrade.DidaTradePaxos.DecidedEntry;
import didatrade.DidaTradePaxos.StateTransferReply;
import didatrade.DidaTradePaxos.StateTransferRequest;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class StateTransfer {
    private final DidaTradeServerState server;
    private final boolean              enabled;

    StateTransfer(DidaTradeServerState server, boolean enabled) {
        this.server  = server;
        this.enabled = enabled;
    }

    public void sendDecidedLog(int ballot) {
        if (!enabled) return;

        StateTransferRequest.Builder request = StateTransferRequest.newBuilder()
                .setBallot(ballot)
                .setServerid(server.my_id);

        for (int position = 0; ; position++) {
            Integer       reqid  = server.paxos_log.getDecidedCommandId(position);
            RequestRecord record = (reqid == null) ? null : server.req_history.getIfExists(reqid);
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
        if (request.getEntriesCount() == 0) return;

        System.out.println("State transfer: sending " + request.getEntriesCount() + " decided entries for ballot " + ballot);

        for (int replica : server.scheduler.learners(ballot)) {
            if (replica == server.my_id) continue;

            server.async_stubs[replica]
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .statetransfer(request.build(), new StreamObserver<>() {
                        public void onNext(StateTransferReply reply) {
                            if (reply.getFilledCount() > 0)
                                System.out.println("State transfer: replica " + replica + " filled " + reply.getFilledCount() + " entries");
                        }
                        public void onError(Throwable error) {
                            System.out.println("State transfer: replica " + replica + " unreachable");
                        }
                        public void onCompleted() {
                        }
                    });
        }
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
