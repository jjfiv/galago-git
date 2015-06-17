// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/*
 * Memory Index
 * 
 * author: sjh, schiu
 * 
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
public class MemoryIndex implements DynamicIndex, Index {

  public boolean stemming, nonstemming, makecorpus, dirty;
  protected int documentNumberOffset, documentCount;
  protected Parameters manifest;
  protected HashMap<String, MemoryIndexPart> parts;
  protected LengthsReader lengthsReader = null;
  protected NamesReader namesReader = null;
  // haven't got any of these at the moment
  HashMap<String, String> defaultIndexOperators = new HashMap<>();
  HashSet<String> knownIndexOperators = new HashSet<>();

  public MemoryIndex(Parameters parameters) throws Exception {
    manifest = parameters;
    // determine which parts are to be created:
    stemming = manifest.get("stemming", true);
    nonstemming = manifest.get("nonstemming", true);
    makecorpus = manifest.get("corpus", manifest.get("makecorpus", false));

    // we should have either a stemmed or non-stemmed posting list
    assert stemming || nonstemming;

    // this allows memory index to start numbering documents from a specific documentCount.
    documentNumberOffset = (int) manifest.get("documentNumberOffset", 0L);
    documentCount = documentNumberOffset;

    // Load all parts
    parts = new HashMap<>();
    Parameters partParams = Parameters.create();
    partParams.set("documentNumberOffset", documentNumberOffset);
    parts.put("names", new MemoryDocumentNames(partParams.clone()));
    parts.put("lengths", new MemoryDocumentLengths(partParams.clone()));
    parts.put("extents", new MemoryWindowIndex(partParams.clone()));

    if (makecorpus) {
      parts.put("corpus", new MemoryCorpus(partParams.clone()));
    }
    if (nonstemming) {
      parts.put("postings", new MemoryPositionalIndex(partParams.clone()));
    }
    if (stemming) {
      Parameters stemParams = partParams.clone();
      // should change this to support several stemmers...
      stemParams.set("stemmer", manifest.get("stemmer", KrovetzStemmer.class.getName()));
      parts.put("postings.krovetz", new MemoryPositionalIndex(stemParams));
    }

    // get a pointer to some special parts:
    lengthsReader = (LengthsReader) parts.get("lengths");
    namesReader = (NamesReader) parts.get("names");

    initializeIndexOperators();
    dirty = false;
  }

  public MemoryIndex() throws Exception {
    this(Parameters.create());
  }

  public MemoryIndex(TupleFlowParameters tfp) throws Exception {
    this(tfp.getJSON());
  }

  @Override
  public String getIndexPath(){
    return "memory";
  }
  
  /**
   * Special function to return the number of documents stored in memory
   *
   * @return documentCount
   */
  public int documentsInIndex() {
    return (documentCount - documentNumberOffset);
  }

  @Override
  public void process(Document doc) throws IOException {
    doc.identifier = documentCount;
    for (MemoryIndexPart part : parts.values()) {
      part.addDocument(doc);
    }
    documentCount++;
  }

  /* this Isn't required at the moment
   *public boolean hasChanged() {
   *  return dirty;
   *}
   */
  @Override
  public String getDefaultPart() {
    if (manifest.isString("defaultPart")) {
      String part = manifest.getString("defaultPart");
      if (parts.containsKey(part)) {
        return part;
      }
    }

    // otherwise, try to default
    if (parts.containsKey("postings.krovetz")) {
      return "postings.krovetz";
    }
    if (parts.containsKey("postings.porter")) {
      return "postings.porter";
    }
    if (parts.containsKey("postings")) {
      return "postings";
    }
    // otherwise - anything will do.
    return parts.keySet().iterator().next();
  }

  /**
   * Tests to see if a named index part exists.
   *
   * @param partName The name of the index part to check.
   * @return true, if this index has a part called partName, or false otherwise.
   */
  @Override
  public boolean containsPart(String partName) {
    return parts.containsKey(partName);
  }

  @Override
  public MemoryIndexPart getIndexPart(String partName) {
    return parts.get(partName);
  }

  void initializeIndexOperators() {
    for (Entry<String, MemoryIndexPart> entry : parts.entrySet()) {
      String partName = entry.getKey();
      IndexPartReader part = entry.getValue();

      for (String name : part.getNodeTypes().keySet()) {
        knownIndexOperators.add(name);

        if (!defaultIndexOperators.containsKey(name)) {
          defaultIndexOperators.put(name, partName);
        } else if (name.startsWith("default")) {
          if (defaultIndexOperators.get(name).startsWith("default")) {
            defaultIndexOperators.remove(name);
          } else {
            defaultIndexOperators.put(name, partName);
          }
        } else {
          defaultIndexOperators.remove(name);
        }
      }
    }
  }

  @Override
  public String getIndexPartName(Node node) throws IOException {
    String operator = node.getOperator();
    String partName = null;

    if (node.getNodeParameters().containsKey("part")) {
      partName = node.getNodeParameters().getString("part");
      if (!parts.containsKey(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
    } else if (knownIndexOperators.contains(operator)) {
      if (!defaultIndexOperators.containsKey(operator)) {
        throw new IOException("More than one index part supplies the operator '"
                + operator + "', but no part name was specified.");
      } else {
        partName = defaultIndexOperators.get(operator);
      }
    }
    return partName;
  }

  @Override
  public BaseIterator getIterator(Node node) throws IOException {
    BaseIterator result = null;
    IndexPartReader part = parts.get(getIndexPartName(node));
    if (part != null) {
      result = part.getIterator(node);
      if (result == null) {
        result = new NullExtentIterator();
      }
    }
    return result;
  }

  @Override
  public NodeType getNodeType(Node node) throws IOException {
    NodeType result = null;
    IndexPartReader part = parts.get(getIndexPartName(node));
    if (part != null) {
      String operator = node.getOperator();
      Map<String, NodeType> nodeTypes = part.getNodeTypes();
      result = nodeTypes.get(operator);
    }
    return result;
  }

  /**
   * WARNING: this function returns a static picture of the collection stats.
   * You should NEVER cache this object, as you will miss any new documents that come through.
   *
   * @param part the index part to query statistics.
   * @return a temporary view of the index-part statistics.
   */
  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    if (parts.containsKey(part)) {
      IndexPartReader p = parts.get(part);
      if (AggregateIndexPart.class.isInstance(p)) {
        return ((AggregateIndexPart) p).getStatistics();
      }
      throw new IllegalArgumentException("Index part, " + part + ", does not store aggregated statistics.");
    }
    throw new IllegalArgumentException("Index part, " + part + ", could not be found in memory index.");
  }

  @Override
  public void close() throws IOException {
    // TESTING: try flushing:
    //(new FlushToDisk()).flushMemoryIndex(this, "./flush/", false);

    for (IndexPartReader part : parts.values()) {
      part.close();
    }
    parts = null;
  }

  @Override
  public boolean containsDocumentIdentifier(long document) throws IOException {
    String n = this.namesReader.getDocumentName(document);
    return (n != null);
  }

  @Override
  public int getLength(long document) throws IOException {
    return lengthsReader.getLength(document);
  }

  @Override
  public String getName(long document) throws IOException {
    return namesReader.getDocumentName(document);
  }

  @Override
  public long getIdentifier(String document) throws IOException {
    throw new UnsupportedOperationException("Memory index does not currently support getIdentifier function.");
  }

  @Override
  public Document getDocument(String document, DocumentComponents p) throws IOException {
    if (parts.containsKey("corpus")) {
      try {
        CorpusReader corpus = (CorpusReader) parts.get("corpus");
        long docId = getIdentifier(document);
        corpus.getDocument(docId, p);
      } catch (Exception e) {
        // ignore the exception
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getDocuments(List<String> documents, DocumentComponents p) throws IOException {
    HashMap<String, Document> results = new HashMap<>();

    // should get a names iterator + sort requested documents
    for (String name : documents) {
      results.put(name, getDocument(name, p));
    }
    return results;
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    return ((MemoryDocumentLengths) parts.get("lengths")).getLengthsIterator();
  }

  @Override
  public DataIterator<String> getNamesIterator() throws IOException {
    return namesReader.getNamesIterator();
  }

  @Override
  public Parameters getManifest() {
    return manifest;
  }

  @Override
  public Set<String> getPartNames() {
    return parts.keySet();
  }

  @Override
  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException {
    if (!parts.containsKey(partName)) {
      throw new IOException("The index has no part named '" + partName + "'");
    }
    return parts.get(partName).getNodeTypes();
  }
}
