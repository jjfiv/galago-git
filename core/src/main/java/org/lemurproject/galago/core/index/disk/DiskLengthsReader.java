// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file. KeyValueIterator
 * provides a useful interface for dumping the contents of the file.
 * 
 * data stored in each document 'field' lengths list:
 * 
 *   stats:
 *  - number of non-zero document lengths (document count)
 *  - sum of document lengths (collection length)
 *  - average document length
 *  - maximum document length
 *  - minimum document length
 * 
 *   utility values:
 *  - first document id
 *  - last document id (all documents inbetween have a value)
 * 
 *   finally:
 *  - list of lengths (one per document)
 *
 * @author sjh
 */
public class DiskLengthsReader extends KeyListReader implements LengthsReader {

  // this is a special memory map for document lengths
  // it is used in the special documentLengths iterator
  private byte[] doc;
  private MappedByteBuffer documentLengths;
  private MemoryMapLengthsIterator documentLengthsIterator;

  public DiskLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    init();
  }

  public DiskLengthsReader(BTreeReader r) throws IOException {
    super(r);
    init();
  }

  public void init() throws IOException {
    doc = Utility.fromString("document");
    documentLengths = reader.getValueMemoryMap(doc);
    documentLengthsIterator = new MemoryMapLengthsIterator(doc, documentLengths);
  }

  @Override
  public int getLength(int document) throws IOException {
    return documentLengthsIterator.getLength(document);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return new MemoryMapLengthsIterator(doc, documentLengths);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(MemoryMapLengthsIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      String key = node.getNodeParameters().get("default", "document");
      KeyIterator ki = new KeyIterator(reader);
      ki.skipToKey(Utility.fromString(key));
      return ki.getValueIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      return "length Data";
    }

    @Override
    public org.lemurproject.galago.core.index.ValueIterator getValueIterator() throws IOException {
      return new MemoryMapLengthsIterator(iterator.getKey(), iterator.getValueMemoryMap());
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public class MemoryMapLengthsIterator extends ValueIterator
          implements MovableCountIterator, LengthsReader.Iterator {

    byte[] key;
    private MappedByteBuffer memBuffer;
    // stats
    private int nonZeroDocumentCount;
    private int collectionLength;
    private double avgLength;
    private int maxLength;
    private int minLength;
    // utility
    private int firstDocument;
    private int lastDocument;
    // iteration vars
    int lengthsDataOffset;
    int currDocument;
    private boolean done;

    public MemoryMapLengthsIterator(byte[] key, MappedByteBuffer data) {
      this.key = key;
      this.memBuffer = data;

      // collect stats
      this.memBuffer.position(0);
      this.nonZeroDocumentCount = data.getInt();
      this.collectionLength = data.getInt();
      this.avgLength = data.getDouble();
      this.maxLength = data.getInt();
      this.minLength = data.getInt();

      this.firstDocument = data.getInt();
      this.lastDocument = data.getInt();

      this.lengthsDataOffset = this.memBuffer.position(); // probably == (4 * 6) + (8)

      // offset is the first document
      this.currDocument = firstDocument;
      this.done = (currDocument > lastDocument);
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public void next() throws IOException {
      currDocument++;
      if (currDocument > lastDocument) {
        currDocument = lastDocument;
        done = true;
      }
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      currDocument = identifier;
      if (currDocument > lastDocument) {
        currDocument = lastDocument;
        done = true;
      }
    }

    @Override
    public void reset() throws IOException {
      this.currDocument = firstDocument;
      this.done = (currDocument > lastDocument);
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public String getEntry() throws IOException {
      return getCurrentIdentifier() + "," + getCurrentLength();
    }

    @Override
    public long totalEntries() {
      return nonZeroDocumentCount;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "lengths";
      String className = this.getClass().getSimpleName();
      String parameters = Utility.toString(key);
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(getCurrentLength());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public int count() {
      return getCurrentLength();
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int getCurrentLength() {
      return getLength(currDocument);
    }

    @Override
    public int getCurrentIdentifier() {
      return this.currDocument;
    }

    private int getLength(int document) {
      // check for range.
      if (firstDocument <= document && document <= lastDocument) {
        return this.memBuffer.getInt(this.lengthsDataOffset + (4 * (document - firstDocument)));
      }
      return 0;
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(key);
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return key;
    }

    @Override
    public boolean hasMatch(int identifier) {
      return !isDone() && this.currDocument == identifier;
    }

    @Override
    public void movePast(int identifier) throws IOException {
      moveTo(identifier + 1);
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
  }

  public class StreamLengthsIterator extends KeyListReader.ListIterator
          implements MovableCountIterator, LengthsReader.Iterator {

    private final BTreeIterator iterator;
    private DataStream streamBuffer;
    // stats
    private int nonZeroDocumentCount;
    private int collectionLength;
    private double avgLength;
    private int maxLength;
    private int minLength;
    // utility
    private int firstDocument;
    private int lastDocument;
    // iteration vars
    private int currDocument;
    private int currLength;
    private long lengthsDataOffset;
    private boolean done;

    public StreamLengthsIterator(byte[] key, BTreeIterator it) throws IOException {
      super(key);
      this.iterator = it;
      reset(it);
    }

    @Override
    public void reset(BTreeIterator it) throws IOException {
      this.streamBuffer = it.getValueStream();

      // collect stats
      this.nonZeroDocumentCount = streamBuffer.readInt();
      this.collectionLength = streamBuffer.readInt();
      this.avgLength = streamBuffer.readDouble();
      this.maxLength = streamBuffer.readInt();
      this.minLength = streamBuffer.readInt();

      this.firstDocument = streamBuffer.readInt();
      this.lastDocument = streamBuffer.readInt();

      this.lengthsDataOffset = this.streamBuffer.getPosition(); // should be == (4 * 6) + (8)

      // offset is the first document
      this.currDocument = firstDocument;
      this.currLength = -1;
      this.done = (currDocument > lastDocument);
    }

    @Override
    public void reset() throws IOException {
      this.reset(iterator);
    }

    @Override
    public int currentCandidate() {
      return this.currDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public void next() throws IOException {
      this.currDocument++;
      if (currDocument > lastDocument) {
        currDocument = lastDocument;
        done = true;
      } else {
        // we only delete the length if we moved
        // this is because we can't re-read the length value
        this.currLength = -1;
      }
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      assert (identifier >= currDocument);

      // we can't move past the last document
      if (identifier > lastDocument) {
        done = true;
        identifier = lastDocument;
      }

      if (currDocument < identifier) {
        // we only delete the length if we move
        // this is because we can't re-read the length value
        currDocument = identifier;
        currLength = -1;
      }
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public String getEntry() throws IOException {
      return getCurrentIdentifier() + "," + getCurrentLength();
    }

    @Override
    public long totalEntries() {
      return this.nonZeroDocumentCount;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "lengths";
      String className = this.getClass().getSimpleName();
      String parameters = Utility.toString(key);
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(getCurrentLength());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public int count() {
      return getCurrentLength();
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int getCurrentLength() {
      // check if we need to read the length value from the stream
      if (this.currLength < 0) {
        // ensure a defaulty value
        this.currLength = 0;
        // check for range.
        if (firstDocument <= currDocument && currDocument < lastDocument) {
          // seek to the required position - hopefully this will hit cache
          this.streamBuffer.seek(lengthsDataOffset + (4 * (this.currDocument - firstDocument)));
          try {
            this.currLength = this.streamBuffer.readInt();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      return currLength;
    }

    @Override
    public int getCurrentIdentifier() {
      return this.currDocument;
    }
  }
}
