// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.util.Bytes;
import org.lemurproject.galago.core.util.IntArray;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class MemoryDocumentLengths implements MemoryIndexPart, LengthsReader {

  private class FieldLengthPostingList {

    private Bytes fieldName;
    private IntArray fieldLengths;
    private int nonZeroDocumentCount = 0;
    private int collectionLength = 0;
    private int maxLength = 0;
    private int minLength = 0;
    private int firstDocument = 0;
    private int lastDocument = 0;

    public FieldLengthPostingList(Bytes fieldName) {
      this.fieldName = fieldName;
      this.fieldLengths = new IntArray(256);
    }

    public void add(int documentId, int fieldLength) throws IOException {
      // initialization step
      if (nonZeroDocumentCount == 0) {
        firstDocument = documentId;
        maxLength = fieldLength;
        minLength = fieldLength;

        // standard insertion stuff.
      } else {
        maxLength = Math.max(fieldLength, this.maxLength);
        minLength = Math.min(fieldLength, this.minLength);
      }
      nonZeroDocumentCount += 1;
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

    private int getLength(int docNum) throws IOException {
      int arrayOffset = docNum - firstDocument;
      if (0 <= arrayOffset && arrayOffset < this.fieldLengths.getPosition()) {
        return fieldLengths.getBuffer()[docNum - firstDocument];
      }
      throw new IOException("Document identifier not found in this index.");
    }
  }
  private Parameters params;
  protected TreeMap<Bytes, FieldLengthPostingList> lengths = new TreeMap();
  private Bytes document;

  public MemoryDocumentLengths(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "org.lemurproject.galago.core.index.DocumentLengthsWriter");
    this.document = new Bytes(Utility.fromString("document"));

    if (!lengths.containsKey(document)) {
      lengths.put(document, new FieldLengthPostingList(document));
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
        lengths.put(field, new FieldLengthPostingList(field));
      }
      lengths.get(field).add(doc.identifier, currentFieldLengths.get(field));
    }
  }

  @Override
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException {
    byte[] fieldString = ((LengthsReader.LengthsIterator) iterator).getRegionBytes();
    Bytes field = new Bytes(fieldString);
    FieldLengthPostingList fieldLengths;
    if (lengths.containsKey(field)) {
      fieldLengths = lengths.get(field);
    } else {
      fieldLengths = new FieldLengthPostingList(field);
    }

    while (!iterator.isDone()) {
      int identifier = ((LengthsReader.LengthsIterator) iterator).getCurrentIdentifier();
      int length = ((LengthsReader.LengthsIterator) iterator).getCurrentLength();
      fieldLengths.add(identifier, length);

      iterator.movePast(identifier);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Lengths iterator data");
  }

  @Override
  public int getLength(int docNum) throws IOException {
    return this.lengths.get(document).getLength(docNum);
  }

  @Override
  public void close() throws IOException {
    lengths = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(FieldLengthsIterator.class));
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
  public LengthsReader.LengthsIterator getLengthsIterator() throws IOException {
    return new FieldLengthsIterator(lengths.get(document));
  }

  @Override
  public ValueIterator getIterator(byte[] key) throws IOException {
    Bytes field = new Bytes(key);
    if (lengths.containsKey(field)) {
      return new FieldLengthsIterator(lengths.get(field));
    }
    // Otherwise make a new (empty) posting list
    return new FieldLengthsIterator(new FieldLengthPostingList(new Bytes(key)));
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
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
      return this.lengths.get(document).nonZeroDocumentCount;
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

    FieldIterator fields = new FieldIterator();
    FieldLengthsIterator fieldLengths;
    FieldLengthData ld;
    while (!fields.isDone()) {
      fieldLengths = (FieldLengthsIterator) fields.getValueIterator();
      while (!fieldLengths.isDone()) {
        ld = new FieldLengthData(fieldLengths.key(), fieldLengths.getCurrentIdentifier(), fieldLengths.getCurrentLength());
        writer.process(ld);
        fieldLengths.movePast(fieldLengths.getCurrentIdentifier());
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

    public boolean skipToKey(int key) throws IOException {
      return findKey(Utility.fromInt(key));
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
    public ValueIterator getValueIterator() throws IOException {
      return new FieldLengthsIterator(lengths.get(new Bytes(this.currField)));
    }
  }

  private static class FieldLengthsIterator extends ValueIterator implements MovableCountIterator, LengthsReader.LengthsIterator, AggregateReader.CollectionAggregateIterator {

    FieldLengthPostingList fieldLengths;
    int currDoc;
    boolean done;

    private FieldLengthsIterator(FieldLengthPostingList fld) throws IOException {
      this.fieldLengths = fld;
      reset();
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(fieldLengths.fieldName.getBytes());
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return fieldLengths.fieldName.getBytes();
    }

    @Override
    public void reset() throws IOException {
      if (this.fieldLengths.nonZeroDocumentCount == 0) {
        this.currDoc = Integer.MAX_VALUE;
        this.done = true;
      } else {
        this.currDoc = fieldLengths.firstDocument;
        this.done = false;
      }
    }

    @Override
    public int currentCandidate() {
      return this.currDoc;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public void movePast(int identifier) throws IOException {
      syncTo(identifier + 1);
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      this.currDoc = Math.min(identifier, this.fieldLengths.lastDocument);
      if (identifier > this.fieldLengths.lastDocument) {
        done = true;
      }
    }

    @Override
    public boolean hasMatch(int identifier) {
      return !done && identifier == this.currDoc;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public long totalEntries() {
      return this.fieldLengths.nonZeroDocumentCount;
    }

    @Override
    public String getEntry() throws IOException {
      return this.getKeyString() + "," + this.getCurrentIdentifier() + "," + this.getCurrentLength();
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "lengths";
      String className = this.getClass().getSimpleName();
      String parameters = this.getKeyString();
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(getCurrentLength());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public int compareTo(MovableIterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    @Override
    public byte[] key() {
      return this.fieldLengths.fieldName.getBytes();
    }

    @Override
    public int count() {
      return this.getCurrentLength();
    }

    @Override
    public int maximumCount() {
      return this.fieldLengths.maxLength;
    }

    @Override
    public byte[] getRegionBytes() {
      return this.key();
    }

    @Override
    public int getCurrentLength() {
      try {
        return this.fieldLengths.getLength(currDoc);
      } catch (IOException ex) {
        Logger.getLogger(this.getClass().getName()).info("Returning 0.\n");
        return 0;
      }
    }

    @Override
    public int getCurrentIdentifier() {
      return this.currDoc;
    }

    @Override
    public CollectionStatistics getStatistics() {
      CollectionStatistics cs = new CollectionStatistics();
      cs.fieldName = Utility.toString(this.fieldLengths.fieldName.getBytes());
      cs.collectionLength = this.fieldLengths.collectionLength;
      cs.documentCount = this.fieldLengths.nonZeroDocumentCount;
      cs.maxLength = this.fieldLengths.maxLength;
      cs.minLength = this.fieldLengths.minLength;
      cs.avgLength = (double) this.fieldLengths.collectionLength / (double) this.fieldLengths.nonZeroDocumentCount;
      return cs;
    }
  }
}
