// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.retrieval.iterator.disk.DiskLengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.SourceIterator;
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
  public int getLength(long document) throws IOException {
    LengthsIterator i = getLengthsIterator();
    ScoringContext sc = new ScoringContext();
    sc.document = document;
    i.setContext(sc);

    i.syncTo(document);
    // will return either the currect length or a zero if no match.
    return i.length();
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    return new DiskLengthsIterator(getLengthsSource());
  }
  
  public DiskLengthSource getLengthsSource() throws IOException {
    BTreeIterator it = reader.getIterator(doc);
    return new DiskLengthSource(it);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(DiskLengthsIterator.class));
    return types;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    // operator -> lengths
    if (node.getOperator().equals("lengths")) {
      String key = node.getNodeParameters().get("default", "document");
      byte[] keyBytes = Utility.fromString(key);
      BTreeIterator i = reader.getIterator(keyBytes);
      return new DiskLengthsIterator(new DiskLengthSource(i));
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
    
    public DiskLengthSource getStreamValueSource() throws IOException {
      return new DiskLengthSource(iterator);
    }
    
    public DiskLengthsIterator getStreamValueIterator() throws IOException {
      return new DiskLengthsIterator(getStreamValueSource());
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }
}
