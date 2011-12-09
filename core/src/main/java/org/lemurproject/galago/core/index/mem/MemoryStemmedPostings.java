// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.ArrayList;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryStemmedPostings extends MemoryPostings {

  Porter2Stemmer stemmer = new Porter2Stemmer();

  public MemoryStemmedPostings(Parameters parameters) {
    super(parameters);
    this.parameters.set("stemming", true);
    this.parameters.set("stemmer", Porter2Stemmer.class.getName());
  }

  @Override
  public Document preProcessDocument(Document doc) throws IOException {
    Document stemdoc = new Document(doc);
    stemmer.stem(stemdoc);
    return stemdoc;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    byte[] stemmedTerm = Utility.fromString(stemmer.stem(node.getDefaultParameter()));
    KeyIterator i = getIterator();
    i.skipToKey(Utility.fromString(node.getDefaultParameter()));
    if ((i.getKeyBytes() != null) && (0 == Utility.compare(i.getKeyBytes(), stemmedTerm))) {
      return i.getValueIterator();
    }
    return null;
  }

  @Override
  public NodeStatistics getTermStatistics(String term) throws IOException {
    return this.getTermStatistics(Utility.fromString(stemmer.stem(term)));
  }
}
