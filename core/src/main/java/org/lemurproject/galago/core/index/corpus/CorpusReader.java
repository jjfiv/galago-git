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
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.PseudoDocument;
import org.lemurproject.galago.core.parse.PseudoDocument.PsuedoDocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.MovableDataIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
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

  boolean psuedoDocs;
  
  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
    psuedoDocs = getManifest().get("psuedo", false);
  }

  public CorpusReader(BTreeReader r) {
    super(r);
    psuedoDocs = getManifest().get("psuedo", false);
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
  public Document getDocument(int key, DocumentComponents p) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    byte[] k = Utility.fromInt(key);
    if (i.findKey(k)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("corpus", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("corpus")) {
      return new CorpusIterator(new KeyIterator(reader));
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
      return Integer.toString(Utility.toInt(getKey()));
    }

    @Override
    public Document getDocument(DocumentComponents p) throws IOException {
      if (psuedoDocs) {
        return PseudoDocument.deserialize(iterator.getValueBytes(), p);
      } else {
        return Document.deserialize(iterator.getValueBytes(), p);
      }
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
    public ValueIterator getValueIterator() throws IOException {
      return new CorpusIterator(this);
    }
  }

  public class CorpusIterator extends KeyToListIterator implements MovableDataIterator<Document> {

    DocumentComponents docParams;

    public CorpusIterator(KeyIterator ki) {
      super(ki);
      docParams = new DocumentComponents();
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getValueString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public Document getData() {
      if (context.document != this.currentCandidate()) {
        try {
          return ((KeyIterator) iterator).getDocument(docParams);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      } else {
        return null;
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public String getKeyString() {
      return "corpus";
    }

    @Override
    public byte[] getKeyBytes() {
      return Utility.fromString("corpus");
    }

    @Override
    public AnnotatedNode getAnnotatedNode() {
      String type = "corpus";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = getData().name;
      String extraInfo = getData().toString();
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extraInfo, children);
    }
  }
}