package didatrade.util;

import java.util.ArrayList;
import java.util.Objects;

public class GenericResponseCollector<T> {
    private GenericResponseProcessor processor;
    private ArrayList<T>             collected_responses;
    private int                      received;
    private int                      pending;
    private boolean                  done;
    private Runnable                 completionCallback;
    private boolean                  completionRegistered;

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses) {
        this.processor           = null;
        this.collected_responses = responses;
        this.received            = 0;
        this.pending             = maxresponses;
        this.done                = false;
    }

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p) {
        this.processor           = p;
        this.collected_responses = responses;
        this.received            = 0;
        this.pending             = maxresponses;
        this.done                = false;
    }

    public void addResponse(T resp) {
        Runnable callback;
        synchronized (this) {
            if (!this.done) {
                collected_responses.add(resp);
                if (this.processor != null) this.done = this.processor.onNext(this.collected_responses, resp);
            }
            this.received++;
            this.pending--;
            if (this.pending == 0) this.done = true;
            notifyAll();
            callback = takeCompletionCallback();
        }
        if (callback != null) callback.run();
    }

    public void addNoResponse() {
        Runnable callback;
        synchronized (this) {
            this.pending--;
            if (this.pending == 0) this.done = true;
            notifyAll();
            callback = takeCompletionCallback();
        }
        if (callback != null) callback.run();
    }

    public void waitForQuorum(int quorum) {
        Runnable callback;
        synchronized (this) {
            while ((this.done == false) && (this.received < quorum)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
            this.done = true;
            notifyAll();
            callback = takeCompletionCallback();
        }
        if (callback != null) callback.run();
    }

    /** Registers one callback, invoked outside this monitor, immediately if already done.
     * A second registration is rejected. Callback exceptions propagate to the invoking thread.
     */
    public void onCompletion(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        Runnable ready;
        synchronized (this) {
            if (completionRegistered) {
                throw new IllegalStateException("A completion callback is already registered.");
            }
            completionRegistered = true;
            completionCallback = callback;
            ready = takeCompletionCallback();
        }
        if (ready != null) ready.run();
    }

    // Called only while holding this monitor. Taking the callback prevents duplicate delivery.
    private Runnable takeCompletionCallback() {
        if (!done) return null;
        Runnable callback = completionCallback;
        completionCallback = null;
        return callback;
    }


    public synchronized void waitUntilDone() {
        while (this.done == false) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
    }

    public synchronized boolean isDone() {
        return done;
    }
}
