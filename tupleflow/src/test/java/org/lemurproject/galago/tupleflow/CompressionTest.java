/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.tupleflow;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 *
 * @author sjh
 */
public class CompressionTest extends TestCase {

  public CompressionTest(String name) {
    super(name);
  }

  public void testCompresion() throws Exception {
    File f1 = Utility.createTemporary();
    File f2 = Utility.createTemporary();
    File f3 = Utility.createTemporary();
    try {
      // write a series of strings to these files.
      Order o = new TupleflowString.ValueOrder();

      FileOrderedWriter<TupleflowString> w1 = new FileOrderedWriter<TupleflowString>(f1.getAbsolutePath(), o, CompressionType.NONE);
      FileOrderedWriter<TupleflowString> w2 = new FileOrderedWriter<TupleflowString>(f2.getAbsolutePath(), o, CompressionType.VBYTE);
      FileOrderedWriter<TupleflowString> w3 = new FileOrderedWriter<TupleflowString>(f3.getAbsolutePath(), o, CompressionType.GZIP);
      for (int i = 0; i < 100; i++) {
        String s = "i=" + i;
        w1.process(new TupleflowString(s));
        w2.process(new TupleflowString(s));
        w3.process(new TupleflowString(s));
      }
      w1.close();
      w2.close();
      w3.close();

      FileOrderedReader<TupleflowString> r1 = new FileOrderedReader(f1.getAbsolutePath());
      FileOrderedReader<TupleflowString> r2 = new FileOrderedReader(f2.getAbsolutePath());
      FileOrderedReader<TupleflowString> r3 = new FileOrderedReader(f3.getAbsolutePath());

      assert (r1.getCompression().equals(CompressionType.NONE));
      assert (r2.getCompression().equals(CompressionType.VBYTE));
      assert (r3.getCompression().equals(CompressionType.GZIP));

      for (int i = 0; i < 100; i++) {
        String s = "i=" + i;
        assert(s.equals(r1.read().value));
        assert(s.equals(r2.read().value));
        assert(s.equals(r3.read().value));
      }
      r1.close();
      r2.close();
      r3.close();

    } finally {
      f1.delete();
      f2.delete();
      f3.delete();
    }
  }
}
