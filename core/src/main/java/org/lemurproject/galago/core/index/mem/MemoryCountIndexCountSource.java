// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.source.MemValueSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.utility.buffer.VByteInput;

/**
 * @author sjh
 */
public class MemoryCountIndexCountSource extends MemValueSource implements CountSource {

  MemoryCountIndex.PostingList postings;
  VByteInput documents_reader;
  VByteInput counts_reader;
  long iteratedDocs;
  long currDocument;
  int currCount;
  boolean done;
  // stats
  private long finalDocument;
  private int finalCount;
  private NodeStatistics stats;

  public MemoryCountIndexCountSource(MemoryCountIndex.PostingList data) throws IOException {
    super(data.key());
    this.postings = data;
    reset();
  }

  @Override
  public void reset() throws IOException {

    // stats extracted from postings
    finalDocument = postings.lastDocument();
    finalCount = postings.lastCount();
    stats = postings.stats();

    documents_reader = new VByteInput(
            new DataInputStream(
            new ByteArrayInputStream(postings.getDocumentDataBytes())));
    counts_reader = new VByteInput(
            new DataInputStream(
            new ByteArrayInputStream(postings.getCountDataBytes())));

    iteratedDocs = 0;
    currDocument = 0;
    currCount = 0;
    
    read();
  }

  @Override
  public int count(long id) {
    if (!done && currentCandidate() == id) {
      return currCount;
    }
    return 0;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public long currentCandidate() {
    return currDocument;
  }

  @Override
  public boolean hasMatch(long identifier) {
    return (!isDone() && identifier == currDocument);
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  private void read() throws IOException {
    if (iteratedDocs >= stats.nodeDocumentCount) {
      done = true;
      return;
    } else if (iteratedDocs == stats.nodeDocumentCount - 1) {
      currDocument = finalDocument;
      currCount = finalCount;
    } else {
      currDocument += documents_reader.readLong();
      currCount = counts_reader.readInt();
    }

    iteratedDocs++;
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    // TODO: need to implement skip lists

    while (!isDone() && (currDocument < identifier)) {
      read();
    }
  }

  @Override
  public void movePast(long identifier) throws IOException {
    syncTo(identifier+1);
  }

  @Override
  public long totalEntries() {
    return stats.nodeDocumentCount;
  }

  @Override
  public NodeStatistics getStatistics() {
    return stats.clone();
  }
}
