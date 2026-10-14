package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoAttempt;
import didatrade.util.PhaseTwoResponseProcessor;

import java.util.*;

public class MultiPaxosMainLoop implements ConsensusLoop {
    private static final long RETRY_DELAY_MILLIS = 500;

    private final DidaTradeServerState                       server_state;
    private       int                                        prepared_ballot     = -1;
    private       Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries    = new HashMap<>();
    private       Map<Integer, Integer>                      assignedRequests    = new HashMap<>();
    private       Map<ProposalKey, ProposalAttempt>          phaseTwoAttempts    = new HashMap<>();
    private       Map<ProposalKey, Long>                     retryNotBefore      = new HashMap<>();
    private       boolean                                    has_work;
    private       int                                        next_new_instance   = 0;
    private       int                                        next_to_execute     = 0;
    private       TreeSet<Integer>                           holes               = new TreeSet<>();
    private       int                                        phaseOneRetryBallot = -1;
    private       long                                       phaseOneRetryNotBefore;

    private record ProposalKey(int ballot, int instance) {
    }

    private record ProposalAttempt(int value, PhaseTwoAttempt attempt) {
    }


    public MultiPaxosMainLoop(DidaTradeServerState state) {
        server_state = state;
        has_work     = false;
    }

    @Override
    public void run() {
        while (true) {
            synchronized (this) {
                retryNotBefore.keySet().removeIf(key -> server_state.paxos_log.isDecided(key.instance()));
                while (!has_work && !retryReady()) {
                    try {
                        long retryDelay = nextRetryDelayMillis();
                        if (retryDelay < 0) wait();
                        else wait(Math.max(1, retryDelay));
                    } catch (InterruptedException e) {
                    }
                }
                has_work = false;
            }

            processCompletedPhaseTwoAttempts();
            executeDecidedPrefix();

            int ballot = server_state.getCurrentBallot();

            if (!prepareLeadership(ballot)) {
                continue;
            }

            // Phase 1 may have overlapped completion of an older attempt.
            processCompletedPhaseTwoAttempts();
            executeDecidedPrefix();

            if (server_state.getCurrentBallot() != ballot) {
                continue;
            }

            startAssignedProposals(ballot);

            while (true) {
                Integer position = assignNextPendingInstance();

                if (position == null) {
                    break;
                }

                startProposal(position, ballot);
            }
        }
    }

    @Override
    public synchronized void wakeup() {
        has_work = true;
        notify();
    }

    private boolean retryReady() {
        long now           = System.currentTimeMillis();
        int  currentBallot = server_state.getCurrentBallot();

        return phaseOneRetryBallot == currentBallot && phaseOneRetryNotBefore <= now
                || retryNotBefore.entrySet().stream()
                .anyMatch(entry -> entry.getKey().ballot() == currentBallot
                        && !server_state.paxos_log.isDecided(entry.getKey().instance())
                        && entry.getValue() <= now);
    }

    private long nextRetryDelayMillis() {
        long now           = System.currentTimeMillis();
        int  currentBallot = server_state.getCurrentBallot();

        long phaseOneDelay = phaseOneRetryBallot == currentBallot
                ? Math.max(0, phaseOneRetryNotBefore - now)
                : Long.MAX_VALUE;
        long phaseTwoDelay = retryNotBefore.entrySet().stream()
                .filter(entry -> entry.getKey().ballot() == currentBallot
                        && !server_state.paxos_log.isDecided(entry.getKey().instance()))
                .mapToLong(entry -> Math.max(0, entry.getValue() - now))
                .min()
                .orElse(Long.MAX_VALUE);
        long delay = Math.min(phaseOneDelay, phaseTwoDelay);
        return delay == Long.MAX_VALUE ? -1 : delay;
    }

    private boolean prepareLeadership(int ballot) {
        if (server_state.getCurrentBallot() != ballot || server_state.scheduler.leader(ballot) != server_state.my_id) {
            return false;
        }

        if (prepared_ballot == ballot) {
            return true;
        }

        if (phaseOneRetryBallot == ballot && phaseOneRetryNotBefore > System.currentTimeMillis()) {
            return false;
        }

        PhaseOneResponseProcessor processor = server_state.runPhaseOne(0, ballot, server_state.getCompletedBallot());

        if (!processor.getAccepted()) {
            server_state.setCurrentBallot(processor.getMaxballot());
            if (server_state.getCurrentBallot() == ballot) {
                phaseOneRetryBallot    = ballot;
                phaseOneRetryNotBefore = System.currentTimeMillis() + RETRY_DELAY_MILLIS;
            }
            return false;
        }

        if (server_state.getCurrentBallot() != ballot) {
            return false;
        }

        phaseOneRetryBallot = -1;
        recoveredEntries    = processor.getRecoveredEntries();

        assignedRequests.clear();
        retryNotBefore.clear();
        holes.clear();
        next_new_instance = next_to_execute;

        Map<Integer, Integer> decidedEntries = server_state.paxos_log.getDecidedEntriesFrom(next_to_execute);
        for (Map.Entry<Integer, Integer> entry : decidedEntries.entrySet()) {
            assignedRequests.put(entry.getKey(), entry.getValue());
            recoveredEntries.remove(entry.getKey());
            next_new_instance = Math.max(next_new_instance, entry.getKey() + 1);
        }

        for (DidaTradePaxos.AcceptedEntry entry : recoveredEntries.values()) {
            if (entry.getInstance() < next_to_execute) continue;
            assignedRequests.put(entry.getInstance(), entry.getValue());

            next_new_instance = Math.max(next_new_instance, entry.getInstance() + 1);
        }

        for (int position = next_to_execute; position < next_new_instance; position++) {
            if (!assignedRequests.containsKey(position)) {
                holes.add(position);
            }

        }
        prepared_ballot = ballot;
        return true;
    }

    private RequestRecord assignPendingRequest(int instance) {

        Integer idreservado = assignedRequests.get(instance);

        if (idreservado != null) {
            return server_state.req_history.getIfPending(idreservado);
        }

        Set<Integer>  excludedIds = new HashSet<>(assignedRequests.values());
        RequestRecord request     = server_state.req_history.getFirstPendingExcluding(excludedIds);

        if (request == null) {
            return null;
        }

        assignedRequests.put(instance, request.getId());
        return request;
    }

    // Returns the reserved log position, or null when no pending request is available.
    private Integer assignNextPendingInstance() {
        boolean fillingHole = !holes.isEmpty();
        int     instance    = fillingHole ? holes.first() : next_new_instance;

        RequestRecord request = assignPendingRequest(instance);
        if (request == null) {
            return null;
        }

        if (fillingHole) {
            holes.remove(instance);
        } else {
            next_new_instance++;
        }

        return instance;
    }

    private void startProposal(int instance, int ballot) {
        ProposalKey key = new ProposalKey(ballot, instance);

        if (phaseTwoAttempts.containsKey(key)) {
            return;
        }

        if (prepared_ballot != ballot
                || server_state.getCurrentBallot() != ballot
                || server_state.scheduler.leader(ballot) != server_state.my_id) {
            return;
        }

        Integer value = selectProposalValue(instance);
        if (value == null) {
            return;
        }

        Long retryDeadline = retryNotBefore.get(key);
        if (retryDeadline != null && retryDeadline > System.currentTimeMillis()) return;

        PhaseTwoAttempt attempt = server_state.startPhaseTwoIfCurrentAndUndecided(instance, ballot, value);
        if (attempt == null) return;

        retryNotBefore.remove(key);
        phaseTwoAttempts.put(key, new ProposalAttempt(value, attempt));
        attempt.onCompletion(this::wakeup);
    }

    private void startAssignedProposals(int ballot) {
        for (int instance : new TreeSet<>(assignedRequests.keySet())) {
            startProposal(instance, ballot);
        }
    }

    private void processCompletedPhaseTwoAttempts() {
        Iterator<Map.Entry<ProposalKey, ProposalAttempt>> iterator = phaseTwoAttempts.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<ProposalKey, ProposalAttempt> entry    = iterator.next();
            ProposalKey                             key      = entry.getKey();
            ProposalAttempt                         proposal = entry.getValue();

            if (!proposal.attempt().isDone()) {
                continue;
            }

            PhaseTwoResponseProcessor result = proposal.attempt().getResult();
            iterator.remove();

            if (!result.getAccepted()) {
                server_state.setCurrentBallot(result.getMaxballot());
                if (server_state.getCurrentBallot() == key.ballot()
                        && !server_state.paxos_log.isDecided(key.instance())) {
                    retryNotBefore.put(key, System.currentTimeMillis() + RETRY_DELAY_MILLIS);
                }
                continue;
            }

            server_state.paxos_log.decideEntry(key.instance(), key.ballot(), proposal.value());
            recoveredEntries.remove(key.instance());
            retryNotBefore.entrySet().removeIf(retry -> retry.getKey().instance() == key.instance());
            server_state.updateCompletedBallot(key.ballot());
        }
    }

    private void executeDecidedPrefix() {
        while (true) {
            Integer commandId = server_state.paxos_log.getDecidedCommandId(next_to_execute);

            if (commandId == null) {
                return;
            }

            if (server_state.req_history.getIfProcessed(commandId) != null) {
                next_to_execute++;
                continue;
            }

            RequestRecord request = server_state.req_history.getIfPending(commandId);

            if (request == null) {
                return;
            }

            server_state.executeRequest(request);
            next_to_execute++;
        }
    }

    private Integer selectProposalValue(int instance) {
        DidaTradePaxos.AcceptedEntry recovered = recoveredEntries.get(instance);

        if (recovered != null) {
            return recovered.getValue();
        }

        RequestRecord request = assignPendingRequest(instance);

        if (request == null) {
            return null;
        }

        return request.getId();
    }


}
