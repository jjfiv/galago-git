// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.lemurproject.galago.core.index.source.MemValueSource;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * @author sjh
 */
public class MemorySparseDoubleIndexScoreSource extends MemValueSource implements ScoreSource {

  MemorySparseDoubleIndex.PostingList postings;
  VByteInput documents_reader;
  VByteInput scores_reader;
  long iteratedDocs;
  long currDocument;
  double currScore;
  boolean done;
  // stats
  private long postingCount;
  private NodeStatistics stats;
  private double defaultScore;
  private double maxScore;
  private double minScore;

  public MemorySparseDoubleIndexScoreSource(MemorySparseDoubleIndex.PostingList data) throws IOException {
    super(data.key());
    this.postings = data;
    reset();
  }

  @Override
  public void reset() throws IOException {

    // stats extracted from postings
    postingCount = postings.postingCount();
    defaultScore = postings.defaultScore();
    defaultScore = postings.maxScore();
    defaultScore = postings.minScore();

    documents_reader = new VByteInput(
            new DataInputStream(
            new ByteArrayInputStream(postings.getDocumentDataBytes())));
    scores_reader = new VByteInput(
            new DataInputStream(
            new ByteArrayInputStream(postings.getScoreDataBytes())));

    currDocument = 0;
    currScore = 0;

    read();
  }

  @Override
  public double score(long id) {
    if (!done && id == currDocument) {
      return currScore;
    } else {
      return defaultScore;
    }
  }

  @Override
  public double maxScore() {
    return maxScore;
  }

  @Override
  public double minScore() {
    return minScore;
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
    if (iteratedDocs >= postingCount) {
      done = true;
      return;
    } else {
      currDocument += documents_reader.readLong();
      currScore = scores_reader.readDouble();
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
    while (!isDone() && (currDocument <= identifier)) {
      read();
    }
  }

  @Override
  public long totalEntries() {
    return postingCount;
  }
}
