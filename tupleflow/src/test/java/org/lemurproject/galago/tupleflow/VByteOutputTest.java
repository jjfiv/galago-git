/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.VByteOutput;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class VByteOutputTest extends TestCase {
    
    public VByteOutputTest(String testName) {
        super(testName);
    }

    public void testWriteString() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        VByteOutput output = new VByteOutput(new DataOutputStream(stream));
        stream.close();

        output.writeString("\u2297");
        byte[] outBytes = stream.toByteArray();
        assertEquals(4, outBytes.length);

        byte[] actualBytes = "\u2297".getBytes("UTF-8");
        assertEquals(actualBytes.length, 3);
        assertEquals(actualBytes[0], outBytes[1]);
        assertEquals(actualBytes[1], outBytes[2]);
        assertEquals(actualBytes[2], outBytes[3]);
    }

}
