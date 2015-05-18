/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.index.mem.MemoryDocumentLengths.FieldLengthList;
import org.lemurproject.galago.core.index.source.LengthSource;
import org.lemurproject.galago.core.index.source.MemValueSource;
import org.lemurproject.galago.core.index.stats.FieldStatistics;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class MemoryDocumentLengthsSource extends MemValueSource implements LengthSource {

  private final FieldLengthList lengthData;
  private long lastDocument;
  private long currDocument;
  private boolean done;
  private FieldStatistics cs;

  public MemoryDocumentLengthsSource(FieldLengthList data) throws IOException {
    super(data.key());
    this.lengthData = data;
    reset();
  }

  @Override
  public void reset() throws IOException {
    currDocument = lengthData.firstDocument();
    lastDocument = lengthData.lastDocument();
    cs = lengthData.stats();
    done = false;
    if (cs.documentCount == 0) {
      done = true;
    }
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public boolean hasAllCandidates() {
    return true; // generally true
  }

  @Override
  public long totalEntries() {
    return cs.documentCount;
  }

  @Override
  public long currentCandidate() {
    return currDocument;
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(long id) throws IOException {
    // check is not really required, but there's a preference not to jump around the list.
    if (id >= currDocument) {
      currDocument = id;
    }
    if (currDocument > lastDocument) {
      done = true;
    }
  }

  @Override
  public int length(long id) {
    if (currDocument == id) {
      try {
        return lengthData.getLength(id);
      } catch (IOException ex) {
        Logger.getLogger(MemoryDocumentLengthsSource.class.getName()).log(Level.SEVERE, "Failed to extract length for " + id, ex);
      }
    }
    return 0;
  }

  @Override
  public FieldStatistics getStatistics() {
    return cs.clone();
  }
}
