// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.core.util.IntArray;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryDocumentLengths implements MemoryIndexPart, LengthsReader {

  private IntArray lengths = new IntArray(256);
  private int offset;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryDocumentLengths(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "org.lemurproject.galago.core.index.DocumentLengthsWriter");
  }

  @Override
  public void addDocument(Document doc) {
    if (lengths.getPosition() == 0) {
      offset = doc.identifier;
    }
    assert (offset + lengths.getPosition() == doc.identifier);
    // otherwise we will have a problem; - or need to add zeros

    docCount += 1;
    termCount += doc.terms.size();
    lengths.add(doc.terms.size());
  }

  @Override
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException {
    while (!iterator.isDone()) {
      int identifier = ((LengthsReader.Iterator) iterator).getCurrentIdentifier();
      int length = ((LengthsReader.Iterator) iterator).getCurrentLength();

      if (lengths.getPosition() == 0) {
        offset = identifier;
      }

      if (offset + lengths.getPosition() > identifier) {
        throw new IOException("Unable to add lengths data out of order.");
      }
      // if we are adding id + lengths directly - we need
      while (offset + lengths.getPosition() < identifier) {
        lengths.add(0);
      }

      docCount += 1;
      termCount += length;
      lengths.add(length);
      iterator.next();
    }
  }

  @Override
  public int getLength(int docNum) {
    int index = docNum - offset;
    assert ((index >= 0) && (index < lengths.getPosition())) : "Document identifier not found in this index.";
    return lengths.getBuffer()[index];
  }

  @Override
  public void close() throws IOException {
    lengths = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(VIterator.class));
    return types;
  }

  @Override
  public String getDefaultOperator() {
    return "lengths";
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return (LengthsReader.Iterator) new VIterator(new KIterator());
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      return new VIterator(getIterator());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public ValueIterator getIterator(byte[] _key) throws IOException {
    return new VIterator(getIterator());
  }

  @Override
  public Parameters getManifest() {
    return params;
  }

  @Override
  public long getDocumentCount() {
    return this.lengths.getPosition();
  }

  @Override
  public long getCollectionLength() {
    return termCount;
  }

  @Override
  public long getKeyCount() {
    return this.lengths.getPosition();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

    KIterator iterator = new KIterator();
    NumberedDocumentData d = new NumberedDocumentData();
    while (!iterator.isDone()) {
      d.number = iterator.getCurrentIdentifier();
      d.textLength = iterator.getCurrentLength();
      writer.process(d);
      iterator.nextKey();
    }
    writer.close();
  }

  public class KIterator implements KeyIterator {

    int current = 0;
    boolean done = false;

    @Override
    public void reset() throws IOException {
      current = 0;
      done = false;
    }

    public int getCurrentIdentifier() {
      return offset + current;
    }

    public int getCurrentLength() throws IOException {
      if (current < lengths.getPosition()) {
        return lengths.getBuffer()[current];
      } else {
        return 0;
      }
    }

    @Override
    public String getKeyString() {
      return Integer.toString(current + offset);
    }

    @Override
    public byte[] getKey() {
      return Utility.fromInt(offset + current);
    }

    @Override
    public boolean nextKey() throws IOException {
      current++;
      if (current >= 0 && current < lengths.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      current = Utility.toInt(key) - offset;
      if (current >= 0 && current < lengths.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      return skipToKey(key);
    }

    @Override
    public String getValueString() throws IOException {
      return Integer.toString(this.getCurrentLength());
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return Utility.fromInt(this.getCurrentLength());
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new VIterator(this);
    }
  }

  private static class VIterator extends KeyToListIterator implements MovableCountIterator, LengthsReader.Iterator {

    public VIterator(KeyIterator it) {
      super(it);
    }

    @Override
    public String getEntry() throws IOException {
      KIterator ki = (KIterator) iterator;
      String output = Integer.toString(ki.getCurrentIdentifier()) + ","
              + Integer.toString(ki.getCurrentLength());
      return output;
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int count() {
      try {
        return ((KIterator) iterator).getCurrentLength();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public int getCurrentLength() {
      KIterator ki = (KIterator) iterator;
      try {
        return ki.getCurrentLength();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public int getCurrentIdentifier() {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public String getKeyString() throws IOException {
      return "lengths";
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return Utility.fromString("lengths");
    }
  }
}
