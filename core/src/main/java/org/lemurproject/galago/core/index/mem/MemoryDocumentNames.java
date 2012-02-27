// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;

import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.core.util.ObjectArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryDocumentNames implements MemoryIndexPart, NamesReader {

  private ObjectArray<String> names = new ObjectArray(String.class, 256);
  private int offset;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryDocumentNames(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "org.lemurproject.galago.core.index.DocumentNameWriter");
  }

  public void addDocument(Document doc) {
    if (names.getPosition() == 0) {
      offset = doc.identifier;
    }
    assert (names.getPosition() + offset == doc.identifier);

    docCount += 1;
    termCount += doc.terms.size();
    names.add(doc.name);
  }

  @Override
  public void addIteratorData(MovableIterator iterator) throws IOException {
    do {
      int identifier = ((NamesReader.Iterator) iterator).getCurrentIdentifier();
      String name = ((NamesReader.Iterator) iterator).getCurrentName();

      if (names.getPosition() == 0) {
        offset = identifier;
      }

      if (offset + names.getPosition() > identifier) {
        throw new IOException("Unable to add names data out of order.");
      }

      // if we are adding id + lengths directly - we need
      while (offset + names.getPosition() < identifier) {
        names.add(null);
      }

      docCount += 1;
      termCount += 1;
      names.add(name);

    } while (iterator.next());
  }

  public String getDocumentName(int docNum) {
    int index = docNum - offset;
    assert ((index >= 0) && (index < names.getPosition())) : "Document identifier not found in this index.";
    return names.getBuffer()[index];
  }

  public int getDocumentIdentifier(String document) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void close() throws IOException {
    names = null;
    offset = 0;
  }

  public NamesReader.Iterator getNamesIterator() throws IOException {
    return new VIterator(new KIterator());
  }

  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  public String getDefaultOperator() {
    return "names";
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(VIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return new VIterator(getIterator());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public Parameters getManifest() {
    return params;
  }

  public long getDocumentCount() {
    return docCount;
  }

  public long getCollectionLength() {
    return termCount;
  }

  public long getVocabCount() {
    return this.names.getPosition();
  }

  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest().clone();
    p.set("filename", path);
    DiskNameWriter writer = new DiskNameWriter(new FakeParameters(p));
    KIterator iterator = new KIterator();
    NumberedDocumentData d;
    ArrayList<NumberedDocumentData> tempList = new ArrayList();
    while (!iterator.isDone()) {
      d = new NumberedDocumentData();
      d.identifier = iterator.getCurrentName();
      d.number = iterator.getCurrentIdentifier();
      writer.process(d);
      tempList.add(d);
      iterator.nextKey();
    }
    writer.close();

    Collections.sort(tempList, new NumberedDocumentData.IdentifierOrder().lessThan());

    p = getManifest().clone();
    p.set("filename", path + ".reverse");
    DiskNameReverseWriter revWriter = new DiskNameReverseWriter(new FakeParameters(p));

    for (NumberedDocumentData ndd : tempList) {
      revWriter.process(ndd);
    }
    revWriter.close();

  }

  public class KIterator implements KeyIterator {

    int current = 0;
    boolean done = false;

    public void reset() throws IOException {
      current = 0;
      done = false;
    }

    public int getCurrentIdentifier() {
      return offset + current;
    }

    public String getCurrentName() throws IOException {
      if (current < names.getPosition()) {
        return names.getBuffer()[current];
      } else {
        return "";
      }
    }

    public String getKeyString() {
      return Integer.toString(current + offset);
    }

    public byte[] getKey() {
      return Utility.fromInt(offset + current);
    }

    public boolean nextKey() throws IOException {
      current++;
      if (current >= 0 && current < names.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    public boolean skipToKey(byte[] key) throws IOException {
      current = Utility.toInt(key) - offset;
      if (current >= 0 && current < names.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    public boolean findKey(byte[] key) throws IOException {
      return skipToKey(key);
    }

    public String getValueString() throws IOException {
      return getCurrentName();
    }

    public byte[] getValueBytes() throws IOException {
      return Utility.fromString(this.getCurrentName());
    }

    public DataStream getValueStream() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isDone() {
      return done;
    }

    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public ValueIterator getValueIterator() throws IOException {
      return new VIterator(this);
    }
  }

  public class VIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.Iterator {

    public VIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      KIterator ki = (KIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getData() {
      try {
        return getEntry();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    public boolean skipToKey(int candidate) throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.skipToKey(candidate);
    }

    public String getCurrentName() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentName();
    }

    public int getCurrentIdentifier() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentIdentifier();
    }
  }
}
