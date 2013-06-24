// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.index.source.MemKeySource;
import org.lemurproject.galago.core.parse.Document;

/**
 *
 * @author sjh
 */
public class MemoryCorpusSource extends MemKeySource<Document> implements DataSource<Document> {

  public MemoryCorpusSource(TreeMap<byte[], Document> corpusData) throws IOException {
    super(corpusData);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "corpus";
  }

  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && currentCandidate() == id);
  }

  @Override
  public Document data(long id) {
    if (currentCandidate() == id) {
      Document d = dataIterator.firstEntry().getValue();
      return d;
    }
    return null;
  }
}
