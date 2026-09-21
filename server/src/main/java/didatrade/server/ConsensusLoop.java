package didatrade.server;

public interface ConsensusLoop extends Runnable {
    void wakeup();
}
