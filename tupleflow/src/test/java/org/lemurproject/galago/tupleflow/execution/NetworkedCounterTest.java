/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.execution.NetworkedCounter;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class NetworkedCounterTest extends TestCase {
    public NetworkedCounterTest(String testName) {
        super(testName);
    }

    public class MockCounter extends NetworkedCounter {
        String connectedUrl;

        MockCounter(String counterName, String stageName, String instance, String url) {
            super(counterName, stageName, instance, url);
        }

        @Override
        public void connectUrl(String url) {
            connectedUrl = url;
        }
    }

    public void testIncrement() {
        MockCounter counter = new MockCounter("a", "b", "c", "d");

        assertEquals(0, counter.count);
        counter.increment();
        assertEquals(1, counter.count);
        counter.increment();
        assertEquals(2, counter.count);
    }

    public void testIncrementBy() {
        MockCounter counter = new MockCounter("a", "b", "c", "d");

        assertEquals(0, counter.count);
        counter.incrementBy(5);
        assertEquals(5, counter.count);
        counter.incrementBy(15);
        assertEquals(20, counter.count);
    }

    public void testFlush() {
        MockCounter counter = new MockCounter("b", "c", "d", "a");
        counter.flush();
        assertEquals("a/setcounter?counterName=b&stageName=c&instance=d&value=0",
                     counter.connectedUrl);
        counter.connectedUrl = null;
        counter.flush();
        // Counter didn't change, so no flush happens.
        assertEquals(null, counter.connectedUrl);
        counter.increment();
        counter.flush();
        assertEquals("a/setcounter?counterName=b&stageName=c&instance=d&value=1",
                     counter.connectedUrl);
        counter.incrementBy(6);
        counter.flush();
        assertEquals("a/setcounter?counterName=b&stageName=c&instance=d&value=7",
                     counter.connectedUrl);
        counter.connectedUrl = null;
        counter.flush();
        // Counter didn't change, so no flush happens.
        assertEquals(null, counter.connectedUrl);
    }

}
