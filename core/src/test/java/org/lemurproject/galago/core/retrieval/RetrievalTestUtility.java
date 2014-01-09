// BSD License (http://lemurproject.org)

package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class RetrievalTestUtility {
    public static int reverseBits(int value, int bitCount) {
        int result = 0;

        for (int i = 0; i < bitCount; i++) {
            boolean bitIsSet = (value & (1 << i)) > 0;

            if (bitIsSet) {
                result |= 1 << (bitCount - i);
            }
        }

        return result;
    }

    /**
     * The goal here is to take in a number that ranges from 1 to 2^maxBits,
     * then turn that into a score.  The score needs to be nicely distributed
     * so that there are a few maximum values and lots of minimum values.
     * 
     * To do this, we flip the document number backwards so that the high bit
     * is now the low bit.  This is kind of like a random number generator, 
     * but it's 1-to-1 with the input number.  Then, we take the log of that number
     * with respect to the maximum.  
     * Most of these log numbers will be high, and a few will be low, so we map
     * low log numbers to high score values.
     */
    public static int documentToScore(int document, int maxScore, int maxBits, int mask) {
        int reversed = reverseBits(document, maxBits);
        int masked = reversed ^ mask;
        int maxDoc = 2 << maxBits;
        double log = Math.log(masked) / Math.log(maxDoc);

        // big log values = low bin values
        return (int) ((1.0 - log) * maxScore);
    }

    public static File createIndexPath() throws IOException {
        // make a spot for the index
        File tempPath = FileUtility.createTemporaryDirectory();

        // put in a generic manifest
        new Parameters().write(tempPath + File.separator + "manifest");

        return tempPath;
    }

    public static TupleFlowParameters createFakeParameters(File tempPath) {
        Parameters p = new Parameters();
        p.set("filename", tempPath.toString());

        return new FakeParameters(p);
    }
}
