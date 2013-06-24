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
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.core.util.ObjectArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryDocumentNames implements MemoryIndexPart, NamesReader {

  private List<String> names = new ArrayList<String>(256);
  private long offset;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryDocumentNames(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "org.lemurproject.galago.core.index.DocumentNameWriter");
  }

  @Override
  public void addDocument(Document doc) {
    if (names.isEmpty()) {
      offset = doc.identifier;
    }

    assert (names.size() + offset <= doc.identifier);
    while (names.size() + offset < doc.identifier) {
      names.add(null); // add nulls to ensure the size of the array is correct
    }

    docCount += 1;
    termCount += doc.terms.size();
    names.add(doc.name);
  }

  @Override
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException {
    while (!iterator.isDone()) {
      long identifier = ((NamesReader.NamesIterator) iterator).getCurrentIdentifier();
      String name = ((NamesReader.NamesIterator) iterator).getCurrentName();

      if (names.isEmpty()) {
        offset = identifier;
      }

      if (offset + names.size() > identifier) {
        throw new IOException("Unable to add names data out of order.");
      }

      // if we are adding id + lengths directly - we need
      while (offset + names.size() < identifier) {
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

  @Override
  public String getDocumentName(long docNum) {
    long index = docNum - offset;
    assert (index < Integer.MAX_VALUE): "Memory index can not store long document ids.";
    assert ((index >= 0) && (index < names.size())) : "Document identifier not found in this index.";
    return names.get((int) index);
  }

  @Override
  public long getDocumentIdentifier(String document) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() throws IOException {
    names = null;
    offset = 0;
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return new MemNamesIterator(new KIterator());
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
    types.put("names", new NodeType(MemNamesIterator.class));
    return types;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return new MemNamesIterator(getIterator());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public DiskIterator getIterator(byte[] _key) throws IOException {
    return new MemNamesIterator(getIterator());
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
    return docCount;
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

    @Override
    public void reset() throws IOException {
      current = 0;
      done = false;
    }

    public int getCurrentIdentifier() {
      // TODO stop casting documents to ints
      return (int) (offset + current);
    }

    public String getCurrentName() throws IOException {
      if (current < names.size()) {
        return names.get(current);
      } else {
        return "";
      }
    }

    @Override
    public String getKeyString() {
      return Long.toString(current + offset);
    }

    @Override
    public byte[] getKey() {
      return Utility.fromLong(current + offset);
    }

    @Override
    public boolean nextKey() throws IOException {
      current++;
      if (current >= 0 && current < names.size()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      current = (int) (Utility.toLong(key) - offset);
      if (current >= 0 && current < names.size()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromLong((long)key));
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      return skipToKey(key);
    }

    @Override
    public String getValueString() throws IOException {
      return getCurrentName();
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return Utility.fromString(this.getCurrentName());
    }

    public DataStream getValueStream() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
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
    public DiskIterator getValueIterator() throws IOException {
      return new MemNamesIterator(this);
    }
  }

  public class MemNamesIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.NamesIterator {

    public MemNamesIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public String getValueString() throws IOException {
      KIterator ki = (KIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
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

    @Override
    public String getCurrentName() throws IOException {
      if (context.document == this.getCurrentIdentifier()) {
        KIterator ki = (KIterator) iterator;
        return ki.getCurrentName();
      }
      // return null by default
      return null;
    }

    @Override
    public long getCurrentIdentifier() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public String getKeyString() throws IOException {
      return "names";
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "names";
      String className = this.getClass().getSimpleName();
      String parameters = this.getKeyString();
      long document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = getCurrentName();
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
