// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;

/**
 * This class holds helper static methods that help with delta encoding for compression.
 * 
 * <p>
 * To encode 'a', with a previous value of 'b', write:
 *    <pre>encode(output, a, b);</pre>
 * and decode as:
 *    <pre>a = decode(input, b);</pre>
 * </p>
 * 
 * <p>
 * For integers, the order of the integers matters.  If the values are ascending,
 * meaning that (last &lt;= current), use encodeAscending/decodeAscending.  If the
 * values are descending, meaning that (last &gt;= current), use encodeDescending/decodeAscending.
 * </p>
 * 
 * @author trevor
 */
public class Delta {
    public static void encodeAscending(ArrayOutput output, String current, String last) throws IOException {
        encode(output, current, last);
    }

    public static void encodeDescending(ArrayOutput output, String current, String last) throws IOException {
        encode(output, current, last);
    }

    public static void encode(ArrayOutput output, String current, String last) throws IOException {
        int maximum = Math.min(current.length(), last.length());
        int i;

        for (i = 0; i < maximum; i++) {
            if (current.charAt(i) != last.charAt(i)) {
                break;
            }
        }

        int overlap = i;

        output.writeInt(overlap);
        output.writeString(last.substring(overlap));
    }

    public static String decodeAscending(ArrayInput input, String last) throws IOException {
        return decode(input, last);
    }

    public static String decodeDescending(ArrayInput input, String last) throws IOException {
        return decode(input, last);
    }

    public static String decode(ArrayInput input, String last) throws IOException {
        int overlap = input.readInt();
        String suffix = input.readString();

        return last.substring(0, overlap) + suffix;
    }

    public static void encodeAscending(ArrayOutput output, long current, long last) throws IOException {
        output.writeLong(current - last);
    }

    public static void encodeDescending(ArrayOutput output, long current, long last) throws IOException {
        output.writeLong(last - current);
    }

    public static long decodeAscending(ArrayInput input, long last) throws IOException {
        return last + input.readLong();
    }

    public static int decodeAscending(ArrayInput input, int last) throws IOException {
        return last + input.readInt();
    }

    public static long decodeDescending(ArrayInput input, long last) throws IOException {
        return last - input.readLong();
    }

    public static int decodeDescending(ArrayInput input, int last) throws IOException {
        return last - input.readInt();
    }

    public static String resetString() {
        return "";
    }

    public static int resetInt() {
        return 0;
    }

    public static long resetLong() {
        return 0;
    }
}
