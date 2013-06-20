// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class SubCollectionWriter implements KeyValuePair.Processor {

  long total;
  long running;
  LinkedList<KeyValuePair> units;
  String path;

  public SubCollectionWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    path = parameters.getJSON().getString("filename");
    units = new LinkedList<KeyValuePair>();
    total = 0;
    running = 0;
  }

  @Override
  public void process(KeyValuePair object) throws IOException {
    total += Utility.uncompressLong(object.value, 0);
    units.add(object);
  }

  @Override
  public void close() throws IOException {
    // Now dump out the statistical information
    OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(path)));
    for (KeyValuePair kvp : units) {
      running += Utility.uncompressLong(kvp.value, 0);
      String filename = new String(kvp.key);
      double pct = (running + 0.0) / total;
      out.write(String.format("%s:%d:%f\n", filename, running, pct));
    }

    out.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("SubCollectionWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }
}
