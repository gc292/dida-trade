package didatrade.configs;

import java.util.List;


public abstract class Schedule {

    abstract List<Integer> learners(int ballot);

    abstract List<Integer> acceptors(int ballot);

    abstract List<Integer> acceptorsinrange(int low_ballot, int high_ballot);

    abstract boolean isacceptor(int node, int ballot);

    abstract List<Integer> allparticipants();

    abstract List<Integer> allparticipantsinballot(int ballot);

    abstract Integer leader(int ballot);

    abstract Integer quorum(int ballot);

    abstract boolean fastpaxos(int ballot);

    // Fast decision quorum for the acceptors of this ballot.
    Integer superMajority(int ballot) {
        int n = acceptors(ballot).size();
        return n - (n - 1) / 3;
    }

    abstract Integer recoveryQuorum(int ballot);
}
