package didatrade.server;

enum DidaTradeAction {POPULATE, ADDUSER, BUY, SELL, BALANCE, DUMP};

public class DidaTradeCommand {
    private DidaTradeAction action;
    private int             user_id;
    private int             quantity;
    private int             stock;

    public DidaTradeCommand(DidaTradeAction command_type) {
        this.action   = command_type;
        this.user_id  = 0;
        this.quantity = 0;
        this.stock    = 0;
    }

    public DidaTradeCommand(DidaTradeAction command_type, int uid) {
        this.action   = command_type;
        this.user_id  = uid;
        this.quantity = 0;
        this.stock    = 0;
    }

    public DidaTradeCommand(DidaTradeAction command_type, int uid, int quantity) {
        this.action   = command_type;
        this.user_id  = uid;
        this.quantity = quantity;
        this.stock    = 0;
    }

    public DidaTradeCommand(DidaTradeAction command_type, int uid, int cash, int stock) {
        this.action   = command_type;
        this.user_id  = uid;
        this.quantity = cash;
        this.stock    = stock;
    }

    // Setter method
    public boolean sameRequestAs(DidaTradeCommand other) {
        return other != null && this.action == other.action
                && this.user_id == other.user_id && this.stock == other.stock
                // BALANCE stores its result in quantity; it has no quantity argument.
                && (this.action == DidaTradeAction.BALANCE || this.quantity == other.quantity);
    }

    public void setQuantity(int value) {
        this.quantity = value;
    }

    // Getter methods for all fields
    public DidaTradeAction getAction() {
        return this.action;
    }

    public int getUserId() {
        return this.user_id;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public int getStock() {
        return this.stock;
    }
}
