/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowLong;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowLong", order = {"+value"})
public class ObjectCounter<T> extends StandardStep<T, TupleflowLong> {

  long count = 0;

  @Override
  public void process(T object) throws IOException {
    count += 1;
  }

  @Override
  public void close() throws IOException {
    processor.process(new TupleflowLong(count));
    processor.close();
  }

  public static String getInputClass(TupleFlowParameters p) {
    return p.getJSON().get("inputClass", "");
  }
}
