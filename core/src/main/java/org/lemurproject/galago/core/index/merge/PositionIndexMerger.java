/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.types.NumberWordPosition;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public class PositionIndexMerger extends GenericExtentValueIndexMerger<NumberWordPosition> {

  public PositionIndexMerger(TupleFlowParameters parameters) throws Exception {
    super(parameters);
  }

  @Override
  public boolean mappingKeys() {
    return false;
  }

  @Override
  public Processor<NumberWordPosition> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    PositionIndexWriter w = new PositionIndexWriter(parameters);
    NumberWordPosition.WordDocumentPositionOrder.TupleShredder shredder = new NumberWordPosition.WordDocumentPositionOrder.TupleShredder(w);
    return shredder;
  }

  public void transformExtentArray(byte[] key, ExtentArray extentArray) throws IOException {
    for (int i = 0; i < extentArray.size(); i++) {
      this.writer.process( new NumberWordPosition( extentArray.getDocument(), key, extentArray.begin(i) ) );
    }
  }
}
