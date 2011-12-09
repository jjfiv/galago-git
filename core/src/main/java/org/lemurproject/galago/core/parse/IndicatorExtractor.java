/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentIndicator;
import org.lemurproject.galago.core.types.NumberKeyValue;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberKeyValue")
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentIndicator")
public class IndicatorExtractor extends StandardStep<NumberKeyValue, DocumentIndicator> {

  @Override
  public void process(NumberKeyValue nkvp) throws IOException {
    if (nkvp.value.length == 0) {
      processor.process(new DocumentIndicator(nkvp.number, true));
    } else {
      processor.process(new DocumentIndicator(nkvp.number,
              Boolean.parseBoolean(Utility.toString(nkvp.value))));
    }
  }
}
