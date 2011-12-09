/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentFeature;
import org.lemurproject.galago.core.types.NumberKeyValue;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Vanilla implementation
 * 
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberKeyValue")
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentFeature")
public class PriorExtractor extends StandardStep<NumberKeyValue, DocumentFeature> {

  private boolean applylog;

  public PriorExtractor(TupleFlowParameters parameters) throws IOException {
    // type of scores being read in:
    String priorType = parameters.getJSON().get("priorType", "raw");
    applylog = false;
    if (priorType.startsWith("prob")) { //prob
      applylog = true;
    }
  }

  @Override
  public void process(NumberKeyValue nkvp) throws IOException {
    if (nkvp.value.length > 0) {
      double val = Double.parseDouble(Utility.toString(nkvp.value));
      if (applylog) {
        val = Math.log(val);
      }
      processor.process(new DocumentFeature(nkvp.number, val));
    }
  }
}
