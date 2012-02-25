package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Map;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Wraps the WindowIndexReader to act as a lengths reader for a
 * particular field.
 * 
 * @author irmarc
 */
public class FieldLengthsReader implements LengthsReader {

  String field;
  WindowIndexReader reader;

  public FieldLengthsReader(WindowIndexReader reader) {
    this.field = "";
    this.reader = reader;
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsReader.Iterator li = getLengthsIterator();
    li.moveTo(document);
    if (li.atCandidate(document)) {
      return li.getCurrentLength();
    } else {
      return 0;
    }
  }

  public void setField(String f) {
    this.field = f;
  }

  public Iterator getLengthsIterator(String f) throws IOException {
    return new LengthIterator(reader.getTermExtents(f));
  }

  @Override
  public Iterator getLengthsIterator() throws IOException {
    return new LengthIterator(reader.getTermExtents(field));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getDefaultOperator() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public org.lemurproject.galago.core.index.KeyIterator getIterator() throws IOException {
    return reader.getIterator();
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Parameters getManifest() {
    return reader.getManifest();
  }

  public class LengthIterator implements LengthsReader.Iterator {

    private WindowIndexReader.TermExtentIterator counts;

    public LengthIterator(WindowIndexReader.TermExtentIterator counts) {
      this.counts = counts;
    }

    @Override
    public boolean skipToKey(int candidate) throws IOException {
      return counts.moveTo(candidate);
    }

    @Override
    public int getCurrentLength() throws IOException {
      int total = 0;
      ExtentArray extents = counts.extents();
      for (int i = 0; i < extents.size(); i++) {
        total += extents.end(i) - extents.begin(i);
      }
      return total;
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      return counts.currentCandidate();
    }

    @Override
    public int currentCandidate() {
      return counts.currentCandidate();
    }

    @Override
    public boolean atCandidate(int identifier) {
      return (counts.currentCandidate() == identifier);
    }

    @Override
    public boolean next() throws IOException {
      return counts.next();
    }

    @Override
    public boolean moveTo(int identifier) throws IOException {
      return counts.moveTo(identifier);
    }

    @Override
    public void movePast(int identifier) throws IOException {
      counts.movePast(identifier);
    }

    @Override
    public String getEntry() throws IOException {
      return counts.getEntry();
    }

    @Override
    public long totalEntries() {
      return counts.totalEntries();
    }

    @Override
    public void reset() throws IOException {
      counts.reset();
    }

    @Override
    public boolean isDone() {
      return counts.isDone();
    }

    @Override
    public int compareTo(ValueIterator t) {
      return counts.compareTo(t);
    }
  }
}
