package didatrade.server;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class RequestHistory {
    private final Map<Integer, RequestRecord>       pending;
    private       Hashtable<Integer, RequestRecord> processed;

    public RequestHistory() {
        this.pending   = new LinkedHashMap<Integer, RequestRecord>();
        this.processed = new Hashtable<Integer, RequestRecord>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
        Integer id = new Integer(requestid);
        return this.pending.get(id);
    }

    public synchronized RequestRecord getFirstPending() {
        for (RequestRecord request : pending.values()) {
            return request;
        }
        return null;
    }

    public synchronized RequestRecord getFirstPendingExcluding(Set<Integer> excludedIds) {
        for (Map.Entry<Integer, RequestRecord> entry : pending.entrySet()) {
            if (!excludedIds.contains(entry.getKey())) {
                return entry.getValue();
            }
        }

        return null;
    }

    public synchronized RequestRecord getIfProcessed(int requestid) {
        Integer id = new Integer(requestid);
        return this.processed.get(id);
    }

    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
        Integer       id = new Integer(requestid);

        record = this.pending.get(id);
        if (record == null) record = this.processed.get(id);
        return record;
    }

    public synchronized RequestRecord addToPending(int requestid, RequestRecord record) {
        Integer id = new Integer(requestid);

        RequestRecord existing = getIfExists(requestid);
        if (existing != null) {
            if (!existing.getRequest().sameRequestAs(record.getRequest())) {
                throw new IllegalArgumentException("Request ID reused with different arguments.");
            }
            return existing;
        }
        this.pending.put(id, record);
        return record;
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
        Integer       id     = new Integer(requestid);
        RequestRecord record = this.pending.remove(id);
        this.processed.put(id, record);
        return record;
    }


}
