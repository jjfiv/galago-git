// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * Reader for corpus folders - corpus folder is a parallel index structure: -
 * one key.index file - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends KeyValueReader implements DocumentReader {

  TagTokenizer tokenizer;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
    init();
  }

  public CorpusReader(BTreeReader r) {
    super(r);
    init();
  }

  public void init() {
    Parameters manifest = getManifest();
    if (manifest.containsKey("tokenizer")) {
      tokenizer = new TagTokenizer(new FakeParameters(getManifest().getMap("tokenizer")));
    } else {
      tokenizer = new TagTokenizer(new FakeParameters(new Parameters()));
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Document getDocument(byte[] key, DocumentComponents p) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    if (i.findKey(key)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Document getDocument(long key, DocumentComponents p) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    byte[] k = Utility.fromLong(key);
    if (i.findKey(k)) {
      return i.getDocument(p);
    } else {
      return null;
    }
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
      return new DiskDataIterator<Document>(new CorpusReaderSource(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator implements DocumentIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(getKey()));
    }

    @Override
    public Document getDocument(DocumentComponents p) throws IOException {
      Document d = Document.deserialize(iterator.getValueBytes(), p);
      if (p.tokenize) {
        tokenizer.tokenize(d);
      }
      return d;
    }

    @Override
    public String getValueString() throws IOException {
      try {
        return getDocument(new Document.DocumentComponents()).toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public DiskDataIterator getValueIterator() throws IOException {
      return new DiskDataIterator(new CorpusReaderSource(reader));
    }
  }
}