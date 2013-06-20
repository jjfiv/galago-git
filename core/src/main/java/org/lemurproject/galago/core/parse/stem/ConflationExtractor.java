/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.parse.stem;

import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class ConflationExtractor extends StandardStep<Document, KeyValuePair> {

  Stemmer stemmer;

  public ConflationExtractor(TupleFlowParameters params) throws Exception {
    String stemmerClass = params.getJSON().getString("stemmerClass");
    stemmer = (Stemmer) Class.forName(stemmerClass).getConstructor().newInstance();
  }

  @Override
  public void process(Document doc) throws IOException {
    for (String term : doc.terms) {
      if (term != null) {
        String stem = stemmer.stem(term);
        if (stem != null) {
          processor.process(new KeyValuePair(Utility.fromString(stem), Utility.fromString(term)));
        }
      }
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("stemmerClass")) {
      store.addError(ConflationExtractor.class.getName() + " requires a stemmerClass parameter.");
    } else {
      try {
        String stemmerClass = parameters.getJSON().getString("stemmerClass");
        Object newInstance = Class.forName(stemmerClass).getConstructor().newInstance();
        Stemmer s = (Stemmer) newInstance;
      } catch (Exception e) {
        store.addError(ConflationExtractor.class.getName() + " failed to get stemmer instance.\n" + e.toString());
      }
    }
  }
}
