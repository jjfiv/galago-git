package org.lemurproject.galago.tupleflow;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

public class LinkageTest {

  @Test
  public void testLinkage() {
    Step start = new Source<TupleflowString>() {
      Processor<TupleflowString> processor;



      @Override
      public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
      }
    };
  }

}