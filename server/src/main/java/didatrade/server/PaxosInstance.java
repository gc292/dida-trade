package didatrade.server;


public class PaxosInstance {
    int     instance_nb;
    int     command_id;
    int     write_ballot;
    int     decision_ballot;
    boolean decided;

    public PaxosInstance() {
        this.instance_nb     = 0;
        this.command_id      = 0;
        this.write_ballot    = -1;
        this.decision_ballot = -1;
        this.decided         = false;
    }


    public PaxosInstance(int id) {
        this.instance_nb     = id;
        this.command_id      = 0;
        this.write_ballot    = -1;
        this.decision_ballot = -1;
        this.decided         = false;
    }
}
