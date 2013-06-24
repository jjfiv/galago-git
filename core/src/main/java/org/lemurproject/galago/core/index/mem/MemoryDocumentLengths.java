// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskLengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.util.Bytes;
import org.lemurproject.galago.core.util.IntArray;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryDocumentLengths implements MemoryIndexPart, LengthsReader {

  public class FieldLengthList {

    private Bytes fieldName;
    private IntArray fieldLengths;
    private long totalDocumentCount = 0;
    private long nonZeroDocumentCount = 0;
    private long collectionLength = 0;
    private long maxLength = 0;
    private long minLength = 0;
    private long firstDocument = 0;
    private long lastDocument = 0;

    public FieldLengthList(Bytes fieldName) {
      this.fieldName = fieldName;
      this.fieldLengths = new IntArray(256);
    }

    public void add(long documentId, int fieldLength) throws IOException {
      // initialization step
      if (totalDocumentCount == 0) {
        firstDocument = documentId;
        maxLength = fieldLength;
        minLength = fieldLength;

        // standard insertion stuff.
      } else {
        maxLength = Math.max(fieldLength, this.maxLength);
        minLength = Math.min(fieldLength, this.minLength);
      }
      totalDocumentCount += 1;
      nonZeroDocumentCount += (fieldLength > 0) ? 1 : 0;
      collectionLength += fieldLength;

      if (firstDocument + fieldLengths.getPosition() > documentId) {
        throw new IOException("Unable to add lengths data out of order.");
      }

      while (firstDocument + fieldLengths.getPosition() < documentId) {
        fieldLengths.add(0);
      }

      fieldLengths.add(fieldLength);
      lastDocument = documentId;
    }

    public int getLength(long docNum) throws IOException {
      long arrayOffset = docNum - firstDocument;
      assert (arrayOffset < Integer.MAX_VALUE) : "Memory index can not store more than Integer.MAX_VALUE document ids.";
      if (0 <= arrayOffset && arrayOffset < this.fieldLengths.getPosition()) {
        // TODO stop casting document to int
        return fieldLengths.getBuffer()[(int) (docNum - firstDocument)];
      }
      throw new IOException("Document identifier not found in this index.");
    }

    public byte[] key() {
      return fieldName.getBytes();
    }

    public long firstDocument() {
      return firstDocument;
    }

    public long lastDocument() {
      return lastDocument;
    }

    public FieldStatistics stats() {
      FieldStatistics cs = new FieldStatistics();
      cs.fieldName = Utility.toString(key());
      cs.collectionLength = collectionLength;
      cs.documentCount = totalDocumentCount;
      cs.nonZeroLenDocCount = nonZeroDocumentCount;
      cs.maxLength = maxLength;
      cs.minLength = minLength;
      cs.avgLength = (double) collectionLength / (double) totalDocumentCount;
      return cs;
    }
  }
  private Parameters params;
  protected TreeMap<Bytes, FieldLengthList> lengths = new TreeMap();
  private Bytes document;

  public MemoryDocumentLengths(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "org.lemurproject.galago.core.index.DocumentLengthsWriter");
    this.document = new Bytes(Utility.fromString("document"));

    if (!lengths.containsKey(document)) {
      lengths.put(document, new FieldLengthList(document));
    }
  }

  @Override
  public void addDocument(Document doc) throws IOException {
    // add the document
    lengths.get(document).add(doc.identifier, doc.terms.size());

    // now deal with fields:
    TObjectIntHashMap<Bytes> currentFieldLengths = new TObjectIntHashMap(doc.tags.size());
    for (Tag tag : doc.tags) {
      int len = tag.end - tag.begin;
      currentFieldLengths.adjustOrPutValue(new Bytes(Utility.fromString(tag.name)), len, len);
    }

    for (Bytes field : currentFieldLengths.keySet()) {
      if (!lengths.containsKey(field)) {
        lengths.put(field, new FieldLengthList(field));
      }
      lengths.get(field).add(doc.identifier, currentFieldLengths.get(field));
    }
  }

  @Override
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException {
    byte[] fieldString = key;
    Bytes field = new Bytes(fieldString);
    FieldLengthList fieldLengths;
    if (lengths.containsKey(field)) {
      fieldLengths = lengths.get(field);
    } else {
      fieldLengths = new FieldLengthList(field);
    }

    while (!iterator.isDone()) {
      long identifier = ((LengthsIterator) iterator).currentCandidate();
      int length = ((LengthsIterator) iterator).length();
      fieldLengths.add(identifier, length);

      // TODO stop casting document to int
      iterator.movePast((int) identifier);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Lengths iterator data");
  }

  @Override
  public int getLength(long docNum) throws IOException {
    return this.lengths.get(document).getLength(docNum);
  }

  @Override
  public void close() throws IOException {
    lengths = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(DiskLengthsIterator.class));
    return types;
  }

  @Override
  public String getDefaultOperator() {
    return "lengths";
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new FieldIterator();
  }

  @Override
  public DiskLengthsIterator getLengthsIterator() throws IOException {
    return new DiskLengthsIterator(new MemoryDocumentLengthsSource(lengths.get(document)));
  }

  @Override
  public DiskLengthsIterator getIterator(byte[] key) throws IOException {
    Bytes field = new Bytes(key);
    if (lengths.containsKey(field)) {
      return new DiskLengthsIterator(new MemoryDocumentLengthsSource(lengths.get(field)));
    }
    // Otherwise make a new (empty) posting list
    return new DiskLengthsIterator(new MemoryDocumentLengthsSource(new FieldLengthList(new Bytes(key))));
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      String fieldName = node.getNodeParameters().get("default", "document");
      return this.getIterator(Utility.fromString(fieldName));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public Parameters getManifest() {
    return params;
  }

  @Override
  public long getDocumentCount() {
    if (lengths.containsKey(document)) {
      return this.lengths.get(document).totalDocumentCount;
    }
    return 0;
  }

  @Override
  public long getCollectionLength() {
    if (lengths.containsKey(document)) {
      return this.lengths.get(document).collectionLength;
    }
    return 0;
  }

  @Override
  public long getKeyCount() {
    return lengths.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

    FieldIterator fields = new FieldIterator(); // key iterator
    DiskLengthsIterator fieldLengths;
    ScoringContext c = new ScoringContext();
    FieldLengthData ld;
    while (!fields.isDone()) {
      fieldLengths = (DiskLengthsIterator) fields.getValueIterator();
      fieldLengths.setContext(c);
      while (!fieldLengths.isDone()) {
        c.document = fieldLengths.currentCandidate();
        ld = new FieldLengthData(Utility.fromString(fieldLengths.getKeyString()), fieldLengths.currentCandidate(), fieldLengths.length());
        writer.process(ld);
        fieldLengths.movePast(fieldLengths.currentCandidate());
      }
      fields.nextKey();
    }
    writer.close();
  }

  public class FieldIterator implements KeyIterator {

    Iterator<Bytes> fields;
    byte[] currField;
    boolean done;

    public FieldIterator() throws IOException {
      reset();
    }

    @Override
    public void reset() throws IOException {
      done = false;
      fields = lengths.keySet().iterator();
      nextKey();
    }

    @Override
    public String getKeyString() {
      return Utility.toString(currField);
    }

    @Override
    public byte[] getKey() {
      return currField;
    }

    @Override
    public boolean nextKey() throws IOException {
      if (fields.hasNext()) {
        currField = fields.next().getBytes();
        return true;
      } else {
        done = true;
        return false;
      }
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      return findKey(key);
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      fields = lengths.tailMap(new Bytes(key)).keySet().iterator();
      return nextKey();
    }

    @Override
    public String getValueString() throws IOException {
      return "Length data can not be output as a string.";
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return null;
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
    public DiskLengthsIterator getValueIterator() throws IOException {
      return new DiskLengthsIterator(new MemoryDocumentLengthsSource(lengths.get(new Bytes(this.currField))));
    }
  }
}
