// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.*;

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
    DataIterator<String> i = (DataIterator<String>) iterator;
    ScoringContext sc = new ScoringContext();
    while (!iterator.isDone()) {

      sc.document = i.currentCandidate();
      String name = i.data(sc);

      if (names.isEmpty()) {
        offset = sc.document;
      }

      if (offset + names.size() > sc.document) {
        throw new IOException("Unable to add names data out of order.");
      }

      // if we are adding id + lengths directly - we need
      while (offset + names.size() < sc.document) {
        names.add(null);
      }

      docCount += 1;
      termCount += 1;
      names.add(name);
      iterator.movePast(sc.document);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Names iterator data");
  }

  @Override
  public String getDocumentName(long docNum) {
    long index = docNum - offset;
    assert (index < Integer.MAX_VALUE) : "Memory index can not store more than Integer.MAX_VALUE document ids.";
    assert ((index >= 0) && (index < names.size())) : "Document identifier not found in this index.";
    return names.get((int) index);
  }

  @Override
  public void close() throws IOException {
    names = null;
    offset = 0;
  }

  @Override
  public DiskDataIterator<String> getNamesIterator() throws IOException {
    return new DiskDataIterator<String>(new MemoryDocumentNamesSource(this.names, this.offset));
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(DiskDataIterator.class));
    return types;
  }

  @Override
  public DiskDataIterator<String> getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return getNamesIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public DiskDataIterator<String> getIterator(byte[] _key) throws IOException {
    return getNamesIterator();
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
    ArrayList<DocumentNameId> tempList = new ArrayList<DocumentNameId>();
    while (!iterator.isDone()) {
      DocumentNameId d = new DocumentNameId();
      d.name = ByteUtil.fromString(iterator.getCurrentName());
      d.id = iterator.getCurrentIdentifier();
      writer.process(d);
      tempList.add(d);
      iterator.nextKey();
    }
    writer.close();

    Collections.sort(tempList, new DocumentNameId.NameOrder().lessThan());

    p = getManifest().clone();
    p.set("filename", path + ".reverse");
    DiskNameReverseWriter revWriter = new DiskNameReverseWriter(new FakeParameters(p));

    for (DocumentNameId ndd : tempList) {
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
      return skipToKey(Utility.fromLong((long) key));
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
      return ByteUtil.fromString(this.getCurrentName());
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
        return CmpUtil.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public DiskDataIterator getValueIterator() throws IOException {
      return new DiskDataIterator<String>(new MemoryDocumentNamesSource(names, offset));
    }
  }
}
