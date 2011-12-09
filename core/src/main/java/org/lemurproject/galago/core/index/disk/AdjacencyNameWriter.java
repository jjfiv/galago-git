// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class AdjacencyNameWriter extends KeyValueWriter<KeyValuePair> {
  public AdjacencyNameWriter(TupleFlowParameters tfp)
          throws FileNotFoundException, IOException {
    super(tfp);
    writer.getManifest().set("writerClass", AdjacencyNameWriter.class.getName());
    writer.getManifest().set("readerClass", AdjacencyNameReader.class.getName());
  }

  protected GenericElement prepare(KeyValuePair item) throws IOException {
    GenericElement ge = new GenericElement(item.key, item.value);
    return ge;
  }

  public static void verify(TupleFlowParameters p, ErrorHandler h) {
    KeyValueWriter.verify(p, h);
  }
}
