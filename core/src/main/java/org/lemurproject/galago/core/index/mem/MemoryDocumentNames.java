// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
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
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException {
    while (!iterator.isDone()) {
      int identifier = ((NamesReader.NamesIterator) iterator).getCurrentIdentifier();
      String name = ((NamesReader.NamesIterator) iterator).getCurrentName();

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
      iterator.movePast(identifier);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Names iterator data");
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

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return new VIterator(new KIterator());
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public String getDefaultOperator() {
    return "names";
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(VIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
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
    return docCount;
  }

  @Override
  public long getCollectionLength() {
    return termCount;
  }

  @Override
  public long getKeyCount() {
    return this.names.getPosition();
  }

  @Override
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

  public class VIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.NamesIterator {

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
        return getCurrentName();
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
      if (context.document == this.getCurrentIdentifier()) {
        KIterator ki = (KIterator) iterator;
        return ki.getCurrentName();
      }
      // return null by default
      return null;
    }

    public int getCurrentIdentifier() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public String getKeyString() throws IOException {
      return "names";
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return Utility.fromString("names");
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "names";
      String className = this.getClass().getSimpleName();
      String parameters = this.getKeyString();
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = getCurrentName();
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
