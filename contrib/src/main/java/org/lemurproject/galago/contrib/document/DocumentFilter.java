/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.document;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;

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
    filter = new HashSet<String>();
    for (String f : p.getAsList("filter", String.class)) {
      BufferedReader reader = Utility.utf8Reader(new File(f));
      String line;

      while ((line = reader.readLine()) != null) {
        filter.add(line.trim());
      }
      reader.close();
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
