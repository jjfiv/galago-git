// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.corpus.CorpusFileWriter;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;

public class MemoryCorpus implements DocumentReader, MemoryIndexPart {

  private TreeMap<byte[], Document> corpusData;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryCorpus(Parameters params) throws IOException {
    this.params = params;
    this.corpusData = new TreeMap(new ByteArrComparator());
  }

  @Override
  public void addDocument(Document doc) {

    docCount += 1;
    termCount += doc.terms.size();

    // save a subset of the document 
    // - to match the output of themake-corpus function.
    corpusData.put(Utility.fromLong(doc.identifier), doc);
  }

  // this is likely to waste all of your memory...
  @Override
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException {
    ScoringContext sc = new ScoringContext();
    while (!iterator.isDone()) {
      sc.document = iterator.currentCandidate();
      Document doc = ((DataIterator<Document>) iterator).data(sc);
      // if the document already exists - no harm done.
      addDocument(doc);
      iterator.movePast(sc.document);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Names iterator data");
  }

  @Override
  public void close() throws IOException {
    // clean up data.
    corpusData = null;
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new MemDocumentIterator(corpusData.keySet().iterator());
  }

  @Override
  public Document getDocument(byte[] key, DocumentComponents p) throws IOException {
    return corpusData.get(key);
  }

  @Override
  public Document getDocument(long key, DocumentComponents p) throws IOException {
    return corpusData.get(Utility.fromLong(key));
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
    return this.corpusData.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    CorpusFileWriter writer = new CorpusFileWriter(new FakeParameters(p));
    DocumentIterator iterator = (DocumentIterator) getIterator();
    while (!iterator.isDone()) {
      writer.process(iterator.getDocument(new DocumentComponents()));
      iterator.nextKey();
    }
    writer.close();
  }

  @Override
  public String getDefaultOperator() {
    return "corpus";
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("corpus", new NodeType(DiskDataIterator.class));
    return types;
  }

  @Override
  public DiskDataIterator<Document> getIterator(Node node) throws IOException {
    if (node.getOperator().equals("corpus")) {
      return new DiskDataIterator(new MemoryCorpusSource(corpusData));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  // unsupported functions - perhaps eventually they will be supported.
  @Override
  public DiskIterator getIterator(byte[] nodeString) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  // document iterator
  public class MemDocumentIterator implements DocumentIterator {

    private Iterator<byte[]> keyIterator;
    private byte[] currKey;

    public MemDocumentIterator(Iterator<byte[]> iterator) throws IOException {
      this.keyIterator = iterator;
      nextKey();
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      keyIterator = corpusData.tailMap(key).keySet().iterator();
      nextKey();
      return (Utility.compare(key, currKey) == 0);
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      keyIterator = corpusData.tailMap(key).keySet().iterator();
      nextKey();
      return (Utility.compare(key, currKey) == 0);
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(currKey));
    }

    @Override
    public byte[] getKey() {
      return currKey;
    }

    @Override
    public boolean isDone() {
      return currKey == null;
    }

    @Override
    public Document getDocument(DocumentComponents p) throws IOException {
      return corpusData.get(currKey);
    }

    @Override
    public boolean nextKey() throws IOException {
      if (keyIterator.hasNext()) {
        currKey = keyIterator.next();
        return true;
      } else {
        currKey = null;
        return false;
      }
    }

    @Override
    public String getValueString() throws IOException {
      return getDocument(new DocumentComponents()).toString();
    }

    @Override
    public void reset() throws IOException {
      keyIterator = corpusData.keySet().iterator();
      nextKey();
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException("Failed to compare mem-corpus keys");
      }
    }

    // unsupported functions:
    @Override
    public DiskDataIterator getValueIterator() throws IOException {
      return new DiskDataIterator(new MemoryCorpusSource(corpusData));
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
