
package didatrade.server;


import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    DidaTradeServerState server_state;

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
        this.server_state = state;
    }

    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
        if (!this.server_state.beforeRequest(responseObserver)) {
            return;
        }
        // System.out.println("Receive phase1 request: \n" + request);

        int                                         instance = request.getInstance();
        int                                         ballot   = request.getRequestballot();
        DidaTradeServerState.PhaseOneAcceptorResult result   = this.server_state.prepareAcceptor(ballot);
        DidaTradePaxos.AcceptedEntry requestedEntry = result.acceptedEntries().stream()
                .filter(entry -> entry.getInstance() == instance)
                .findFirst()
                .orElse(null);

        // System.out.println("Instance = " + instance + " ballot = " + ballot + " current_ballot = " + this.server_state.getCurrentBallot() + " val = " + value + " valballot = " + valballot + " maxballot = " + maxballot + " accepted = " + accepted);

        DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
        response_builder.setInstance(instance);
        response_builder.setServerid(this.server_state.my_id);
        response_builder.setRequestballot(ballot);
        response_builder.setAccepted(result.accepted());
        response_builder.setValue(requestedEntry == null ? 0 : requestedEntry.getValue());
        response_builder.setValballot(requestedEntry == null ? -1 : requestedEntry.getValballot());
        response_builder.setMaxballot(result.maxBallot());

        if (result.accepted()) response_builder.addAllAcceptedEntries(result.acceptedEntries());


        DidaTradePaxos.PhaseOneReply response = response_builder.build();

        // System.out.println("Sending phase1 response: " + response);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
        if (!this.server_state.beforeRequest(responseObserver)) {
            return;
        }
        // System.out.println ("Receive phase two request: \n" + request);

        int instance = request.getInstance();
        int ballot   = request.getRequestballot();
        int value    = request.getValue();
        boolean any = request.getAny();
        if (any && !this.server_state.getFastPaxosMode()) {
            responseObserver.onError(io.grpc.Status.FAILED_PRECONDITION
                    .withDescription("ANY requires Fast Paxos mode").asRuntimeException());
            return;
        }
        DidaTradeServerState.PhaseTwoAcceptorResult result = any
                ? this.server_state.authorizeFastInstance(instance, ballot)
                : this.server_state.acceptPhaseTwo(instance, ballot, value);


        DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
        response_builder.setAccepted(result.accepted());
        response_builder.setInstance(instance);
        response_builder.setServerid(this.server_state.my_id);
        response_builder.setRequestballot(ballot);
        response_builder.setMaxballot(result.maxBallot());

        DidaTradePaxos.PhaseTwoReply response = response_builder.build();

        // System.out.println("Sending phase2 response: " + response);

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        if (result.accepted()) {
            if (any) this.server_state.main_loop.wakeup();
            else this.server_state.notifyLearners(instance, ballot, value);
        }


    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
        if (!this.server_state.beforeRequest(responseObserver)) {
            return;
        }
        // System.out.println("Receive learn request: \n" + request);

        int instance = request.getInstance();
        int ballot   = request.getBallot();
        int value    = request.getValue();
        int acceptor = request.getServerid();

        this.server_state.setCurrentBallot(ballot);
        boolean decided = this.server_state.paxos_log.recordLearn(
                instance, ballot, value, acceptor, this.server_state.scheduler.quorum(ballot));

        if (decided) {
            System.out.println("Paxos learner: waking up the main loop");
            this.server_state.updateCompletedBallot(ballot);
            this.server_state.main_loop.wakeup();
        }

        DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
        response_builder.setInstance(instance);
        response_builder.setBallot(ballot);

        DidaTradePaxos.LearnReply response = response_builder.build();

        // System.out.println("Sending learn response");

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
