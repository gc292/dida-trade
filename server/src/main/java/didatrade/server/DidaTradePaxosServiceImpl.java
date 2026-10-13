
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

        int                                         instance = request.getInstance();
        int                                         ballot   = request.getRequestballot();
        DidaTradeServerState.PhaseOneAcceptorResult result   = this.server_state.prepareAcceptor(ballot);
        DidaTradePaxos.AcceptedEntry requestedEntry = null;
        for (DidaTradePaxos.AcceptedEntry entry : result.acceptedEntries()) {
            if (entry.getInstance() == instance) {
                requestedEntry = entry;
                break;
            }
        }


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


        responseObserver.onNext(response);
        responseObserver.onCompleted();
        if (this.server_state.getFastPaxosMode()) this.server_state.main_loop.wakeup();
    }

    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
        if (!this.server_state.beforeRequest(responseObserver)) {
            return;
        }

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


        responseObserver.onNext(response);
        responseObserver.onCompleted();

        if (result.accepted()) {
            if (any) this.server_state.main_loop.wakeup();
            else {
                this.server_state.notifyLearners(instance, ballot, value);
                if (this.server_state.getFastPaxosMode()) this.server_state.main_loop.wakeup();
            }
        }


    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
        if (!this.server_state.beforeRequest(responseObserver)) {
            return;
        }

        int instance = request.getInstance();
        int ballot   = request.getBallot();
        int value    = request.getValue();
        int acceptor = request.getServerid();

        if (this.server_state.getFastPaxosMode()) {
            PaxosLog.FastLearnResult result = this.server_state.paxos_log.recordFastLearn(
                    instance, ballot, value, acceptor, this.server_state.getFastQuorum(ballot),
                    this.server_state.scheduler.acceptors(ballot));
            this.server_state.setCurrentBallot(ballot);
            if (result == PaxosLog.FastLearnResult.COLLISION) {
                ((FastPaxosMainLoop) this.server_state.main_loop).onCollision(instance, ballot);
            } else if (result == PaxosLog.FastLearnResult.DECIDED) {
                this.server_state.updateCompletedBallot(ballot);
                this.server_state.main_loop.wakeup();
            }
        } else {
            this.server_state.setCurrentBallot(ballot);
            boolean decided = this.server_state.paxos_log.recordLearn(
                    instance, ballot, value, acceptor, this.server_state.scheduler.quorum(ballot));
            if (decided) {
                this.server_state.updateCompletedBallot(ballot);
                this.server_state.main_loop.wakeup();
            }
        }

        DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
        response_builder.setInstance(instance);
        response_builder.setBallot(ballot);

        DidaTradePaxos.LearnReply response = response_builder.build();


        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
