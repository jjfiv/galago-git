/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.document;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class DocumentFilter extends StandardStep<Document, Document> {
  
  Counter passingDocuments;
  HashSet<String> filter;
  boolean require;
  
  public DocumentFilter(TupleFlowParameters tp) throws IOException {
    passingDocuments = tp.getCounter("Retained Documents");
    
    Parameters p = tp.getJSON();
    filter = new HashSet();
    for (String f : (List<String>) p.getAsList("filter")) {
      filter.addAll(Utility.readFileToStringSet(new File(f)));
    }
    
    
    require = p.getBoolean("require");
  }
  
  @Override
  public void process(Document doc) throws IOException {
    // ~xor : (require && filter.contains) || !require && !filter.contains)
    if (!(require ^ filter.contains(doc.name))) {
      if (passingDocuments != null) {
        passingDocuments.increment();
      }
      processor.process(doc);
    }
  }
}
