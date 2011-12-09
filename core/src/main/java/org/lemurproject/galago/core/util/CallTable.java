// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import gnu.trove.TObjectLongHashMap;
import gnu.trove.TObjectLongProcedure;
import java.io.PrintStream;

/**
 * Small class that can be used to track number of calls to methods
 *
 * @author irmarc
 */
public class CallTable {
    private static TObjectLongHashMap<String> counts = new TObjectLongHashMap<String>();
    private static boolean on = true;
    private CallTable() {}

    public static void set(String counterName, long value) {
	if (on) {
	    counts.put(counterName, value);
	}
    }

    public static void increment(String counterName, long inc) {
        if (on) {
            counts.adjustOrPutValue(counterName, inc, inc);
        }
    }

    public static void increment(String counterName) {
        if (on) {
            counts.adjustOrPutValue(counterName, 1, 1);
        }
    }

    public static void reset() {
        counts.clear();
    }

    public static void print(PrintStream out) {
	print(out, "");
    }

    public static void print(PrintStream out, String prefix) {
        Printer p = new Printer(out, prefix);
        counts.forEachEntry(p);
    }

    public static void turnOn() { on = true; }
    public static void turnOff() { on = false; }
    public static boolean getStatus() { return on; }

    private static class Printer implements TObjectLongProcedure {
        PrintStream out;
	String prefix;

        public Printer(PrintStream out, String prefix) {
            this.out = out;
	    this.prefix = prefix;
        }
	
        public boolean execute(Object a, long b) {
            out.printf("CALL_TABLE:%s\t%s\t%d\n", (prefix == null ? "" : "\t"+prefix), a, b);
            return true;
        }
    }
}
