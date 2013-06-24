// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.index.source.DataSource;

/**
 *
 * @author sjh
 */
public class MemoryDocumentNamesSource implements DataSource<String> {

  private final List<String> names;
  private final long offset;
  private int currentIndex;

  public MemoryDocumentNamesSource(List<String> names, long offset) throws IOException {
    this.names = names;
    this.offset = offset;
    reset();
  }

  @Override
  public void reset() throws IOException {
    this.currentIndex = 0;
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "names";
  }

  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && currentCandidate() == id);
  }

  @Override
  public String data(long id) {
    if (currentCandidate() == id && currentIndex < names.size()) {
      return names.get(currentIndex);
    }
    return null;
  }

  @Override
  public boolean isDone() {
    return (currentIndex >= names.size());
  }

  @Override
  public long totalEntries() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long currentCandidate() {
    return currentIndex + offset;
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(long id) throws IOException {
    long tmp = (id < offset) ? 0 : (id - offset);
    assert (tmp < Integer.MAX_VALUE) : "Error in MemoryDocumentNamesSource -- index value is larger than Int";
    currentIndex = (int) tmp;
  }
}
