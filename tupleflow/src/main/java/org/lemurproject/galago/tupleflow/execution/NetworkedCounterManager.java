// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.util.HashMap;
import org.lemurproject.galago.tupleflow.Counter;

/**
 *
 * @author trevor
 */
public class NetworkedCounterManager implements Runnable {
    HashMap<String, NetworkedCounter> counters = new HashMap<String, NetworkedCounter>();
    boolean stop = false;
    int sleepInterval = 1000;
    Thread thread;

    public synchronized Counter newCounter(
            String counterName, String stageName, String instance, String url) {
        String key = String.format("%s-%s-%s", counterName, stageName, instance);
        if (counters.containsKey(key))
            return counters.get(key);
        NetworkedCounter counter = new NetworkedCounter(counterName, stageName, instance, url);
        counters.put(key, counter);
        return counter;
    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    public synchronized void stop() {
        stop = true;
        if (thread != null)
            thread.interrupt();
    }

    public void run() {
        while (true) {
            synchronized(this) {
                for (NetworkedCounter counter : counters.values()) {
                    counter.flush();
                }

                if (stop) break;
            }

            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException ex) {
                // it's probably time to flush and quit now
            }
        }
    }
}
