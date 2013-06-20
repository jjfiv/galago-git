// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.CollectionStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.SourceIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
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
 * stats: - number of non-zero document lengths (document count) - sum of
 * document lengths (collection length) - average document length - maximum
 * document length - minimum document length
 *
 * utility values: - first document id - last document id (all documents
 * inbetween have a value)
 *
 * finally: - list of lengths (one per document)
 *
 * @author irmarc
 * @author sjh
 */
public class DiskLengthsReader extends KeyListReader implements LengthsReader {

  // this is a special memory map for document lengths
  // it is used in the special documentLengths iterator
  private byte[] doc;
//  private MappedByteBuffer documentLengths;
//  private MemoryMapLengthsIterator documentLengthsIterator;

  public DiskLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    init();
  }

  public DiskLengthsReader(BTreeReader r) throws IOException {
    super(r);
    init();
  }

  public void init() throws IOException {
    if (!reader.getManifest().get("emptyIndexFile", false)) {
      doc = Utility.fromString("document");
//      documentLengths = reader.getValueMemoryMap(doc);
//      documentLengthsIterator = new MemoryMapLengthsIterator(doc, documentLengths);
    }
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsIterator i = getLengthsIterator();
    ScoringContext sc = new ScoringContext();
    sc.document = document;
    i.setContext(sc);

    i.syncTo(document);
    // will return either the currect length or a zero if no match.
    return i.getCurrentLength();
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    BTreeIterator i = reader.getIterator(doc);
    return new StreamLengthsIterator(i);
//    return new MemoryMapLengthsIterator(doc, documentLengths);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(StreamLengthsIterator.class));
    return types;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    // operator -> lengths
    if (node.getOperator().equals("lengths")) {
      String key = node.getNodeParameters().get("default", "document");
      byte[] keyBytes = Utility.fromString(key);
      BTreeIterator i = reader.getIterator(keyBytes);
      return new StreamLengthsIterator(i);
    } else {
      throw new UnsupportedOperationException("Index doesn't support operator: " + node.getOperator());
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
    public DiskIterator getValueIterator() throws IOException {
      return getStreamValueIterator();
    }

    //public StreamLengthsIterator getStreamValueIterator() throws IOException {
      //return new StreamLengthsIterator(iterator.getKey(), iterator);
    //}
    
    public StreamLengthsIterator getStreamValueIterator() throws IOException {
      return new StreamLengthsIterator(iterator);
    }

//    public MemoryMapLengthsIterator getMemoryValueIterator() throws IOException {
//      return new MemoryMapLengthsIterator(iterator.getKey(), iterator);
//    }
    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }
  
  public class StreamLengthsSource extends BTreeValueSource implements CountSource {
    DataStream streamBuffer;
    // stats
    long totalDocumentCount;
    long nonZeroDocumentCount;
    long collectionLength;
    double avgLength;
    long maxLength;
    long minLength;
    // utility
    int firstDocument;
    int lastDocument;
    // iteration vars
    int currDocument;
    int currLength;
    long lengthsDataOffset;
    boolean done;

    public StreamLengthsSource(BTreeReader.BTreeIterator iter) throws IOException {
      super(iter);
    }
    
    @Override
    public void reset() throws IOException {
      this.streamBuffer = btreeIter.getValueStream();
      
      // collect stats
      //** temporary fix - this allows current indexes to continue to work **/
      if (btreeIter.reader.getManifest().get("version", 1) == 3) {
        this.totalDocumentCount = streamBuffer.readLong();
        this.nonZeroDocumentCount = streamBuffer.readLong();
        this.collectionLength = streamBuffer.readLong();
        this.avgLength = streamBuffer.readDouble();
        this.maxLength = streamBuffer.readLong();
        this.minLength = streamBuffer.readLong();
      } else if (btreeIter.reader.getManifest().get("longs", false)) {
        this.nonZeroDocumentCount = streamBuffer.readLong();
        this.collectionLength = streamBuffer.readLong();
        this.avgLength = streamBuffer.readDouble();
        this.maxLength = streamBuffer.readLong();
        this.minLength = streamBuffer.readLong();
        this.totalDocumentCount = this.nonZeroDocumentCount;
      } else {
        this.nonZeroDocumentCount = streamBuffer.readInt();
        this.collectionLength = streamBuffer.readInt();
        this.avgLength = streamBuffer.readDouble();
        this.maxLength = streamBuffer.readInt();
        this.minLength = streamBuffer.readInt();
        this.totalDocumentCount = this.nonZeroDocumentCount;
      }

      this.firstDocument = streamBuffer.readInt();
      this.lastDocument = streamBuffer.readInt();

      this.lengthsDataOffset = this.streamBuffer.getPosition(); // should be == (4 * 6) + (8)

      // offset is the first document
      this.currDocument = firstDocument;
      this.currLength = -1;
      this.done = (currDocument > lastDocument);
    }
    
    @Override
    public long count(int id) {
      return getCurrentLength(id);
    }

    @Override
    public boolean isDone() {
      return this.done;
    }

    @Override
    public int currentCandidate() {
      return this.currDocument;
    }

    @Override
    public void movePast(int identifier) {
      // select the next document:
      identifier += 1;

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
    
    public int getCurrentLength(int document) {
      if (document == this.currDocument) {
        // check if we need to read the length value from the stream
        if (this.currLength < 0) {
          // ensure a defaulty value
          this.currLength = 0;
          // check for range.
          if (firstDocument <= currDocument && currDocument <= lastDocument) {
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
      } else {
        return 0;
      }
    }

    @Override
    public void syncTo(int identifier) {
      // it's possible that the first document has zero length, and we may wish to sync to it.
      if (identifier < firstDocument) {
        return;
      }

      assert (identifier >= currDocument) : "StreamLengthsIterator reader can't move to a previous document.";

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
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public long totalEntries() {
      return this.totalDocumentCount;
    }
  }
  
  public class StreamLengthsIterator extends SourceIterator<StreamLengthsSource>
    implements CountIterator, LengthsIterator, CollectionAggregateIterator {
    
    StreamLengthsIterator(BTreeIterator it) throws IOException {
      super(new StreamLengthsSource(it));
    }

    @Override
    public String getValueString() throws IOException {
      return currentCandidate() + "," + getCurrentLength();
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "lengths";
      String className = this.getClass().getSimpleName();
      String parameters = getKeyString();
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(getCurrentLength());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
    
    @Override
    public int count() {
      return (int) source.count(context.document);
    }
    
    @Override
    public int getCurrentLength() {
      return count();
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public CollectionStatistics getStatistics() {
      CollectionStatistics cs = new CollectionStatistics();
      cs.fieldName = source.key();
      cs.collectionLength = source.collectionLength;
      cs.documentCount = source.totalDocumentCount;
      cs.nonZeroLenDocCount = source.nonZeroDocumentCount;
      cs.maxLength = source.maxLength;
      cs.minLength = source.minLength;
      cs.avgLength = source.avgLength;
      return cs;
    }
  }
}
