/*
 *  BSD License (http://lemurproject.org/galago-license)
 */

package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public class WindowIndexMerger extends GenericExtentValueIndexMerger<NumberedExtent> {

  public WindowIndexMerger(TupleFlowParameters parameters) throws Exception{
    super(parameters);
  }

  @Override
  public boolean mappingKeys() {
    return false;
  }
  
  @Override
  public Processor<NumberedExtent> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    WindowIndexWriter w = new WindowIndexWriter(parameters);
    NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder shredder = new NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder( w );
    return shredder;
  }
  
  public void transformExtentArray(byte[] key, ExtentArray extentArray)  throws IOException {
    for(int i=0; i <extentArray.size() ; i++){
      this.writer.process(new NumberedExtent(key, extentArray.getDocument(), extentArray.begin(i), extentArray.end(i)) );
    }
  }
}
