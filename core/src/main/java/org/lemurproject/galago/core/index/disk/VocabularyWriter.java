// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * [sjh] this class could cause problems for VERY large vocabularies
 *
 * @author trevor
 */
public class VocabularyWriter {

  DataOutputStream output;
  ByteArrayOutputStream buffer;

  public VocabularyWriter() throws IOException {
    buffer = new ByteArrayOutputStream();
    output = new DataOutputStream(new BufferedOutputStream(buffer));
  }

  public void add(byte[] key, long offset, int headerLength) throws IOException {
    Utility.compressInt(output, key.length);
    output.write(key);
    Utility.compressLong(output, offset);
    Utility.compressInt(output, headerLength);
  }

  public byte[] data() throws IOException {
    output.close();
    return buffer.toByteArray();
  }
}
