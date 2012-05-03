/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.mem.MemoryCorpus;
import org.lemurproject.galago.core.index.mem.MemoryDocumentLengths;
import org.lemurproject.galago.core.index.mem.MemoryDocumentNames;
import org.lemurproject.galago.core.index.mem.MemoryIndexPart;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class CachedDiskIndex implements Index {

  // disk index - fall back
  DiskIndex diskIndex;
  // memory parts
  LengthsReader memLengthsReader;
  NamesReader memNamesReader;
  Map<String, MemoryIndexPart> memParts;

  public CachedDiskIndex(String indexPath) throws IOException {
    // load disk index
    diskIndex = new DiskIndex(indexPath);

    // load lengths
    memLengthsReader = new MemoryDocumentLengths(diskIndex.lengthsReader.getManifest());
    LengthsReader.Iterator lengthsData = diskIndex.lengthsReader.getLengthsIterator();
    ((MemoryDocumentLengths) memLengthsReader).addIteratorData(new byte[0], lengthsData);

    // load names
    memNamesReader = new MemoryDocumentNames(diskIndex.namesReader.getManifest());
    NamesReader.Iterator namesData = diskIndex.namesReader.getNamesIterator();
    ((MemoryDocumentNames) memNamesReader).addIteratorData(new byte[0], namesData);

    memParts = new HashMap();
    memParts.put("lengths", (MemoryDocumentLengths) memLengthsReader);
    memParts.put("names", (MemoryDocumentNames) memNamesReader);
  }

  @Override
  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return memLengthsReader.getLengthsIterator();
  }

  @Override
  public NamesReader.Iterator getNamesIterator() throws IOException {
    return memNamesReader.getNamesIterator();
  }

  @Override
  public int getLength(int document) throws IOException {
    return memLengthsReader.getLength(document);
  }

  @Override
  public String getName(int document) throws IOException {
    return memNamesReader.getDocumentName(document);
  }

  @Override
  public boolean containsDocumentIdentifier(int document) throws IOException {
    NamesReader.Iterator ni = getNamesIterator();
    ni.moveTo(document);
    return ni.atCandidate(document);
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    // try to use in-memory indexes first
    ValueIterator result = null;
    IndexPartReader part = memParts.get(getIndexPartName(node));
    if (part != null) {
      result = part.getIterator(node);
      modify(result, node);
      if (result != null) {
        return result;
      }
    }
    result = diskIndex.getIterator(node);
    return result;
  }

  @Override
  public void close() throws IOException {
    diskIndex.close();
    for (MemoryIndexPart p : this.memParts.values()) {
      p.close();
    }
    this.memParts.clear();
    this.memNamesReader = null;
    this.memLengthsReader = null;
  }

  @Override
  public Document getDocument(String document, Parameters p) throws IOException {
    if (memParts.containsKey("corpus")) {
      try {
        MemoryCorpus corpus = (MemoryCorpus) memParts.get("corpus");
        int docId = getIdentifier(document);
        return corpus.getDocument(docId, p);
      } catch (Exception e) {
        // ignore the exception
      }
      return null;
    } else {
      return diskIndex.getDocument(document, p);
    }
  }

  @Override
  public Map<String, Document> getDocuments(List<String> documents, Parameters p) throws IOException {
    HashMap<String, Document> results = new HashMap();

    // should get a names iterator + sort requested documents
    for (String name : documents) {
      results.put(name, getDocument(name, p));
    }
    return results;
  }

  // Caching functions
  public void cacheQueryData(List<Node> queryTrees) throws IOException {
    // for each query tree
    for (Node queryTree : queryTrees) {
      cacheQueryData(queryTree);
    }
  }

  public void cacheQueryData(Node queryNode) throws IOException {
    // for each node in query
    for (Node internalNode : queryNode.getInternalNodes()) {
      cacheQueryData(internalNode);
    }

    // use diskIndex to create an iterator
    ValueIterator iterator;
    if (queryNode.getOperator().equals("counts")) {
      // KNOWN ISSUE -- #extents may be replaced by a #counts node
      Node extentsNode = new Node("extents", queryNode.getNodeParameters(), queryNode.getInternalNodes(), queryNode.getPosition());
      iterator = diskIndex.getIterator(extentsNode);
    } else {
      iterator = diskIndex.getIterator(queryNode);
    }

    if (iterator != null) {
      // check if the part is already buffered
      String partName = diskIndex.getIndexPartName(queryNode);      
      if (!memParts.containsKey(partName)) {
        IndexPartReader partReader = diskIndex.parts.get(partName);
        String memoryClassName = partReader.getManifest().get("memoryClass", null);
        MemoryIndexPart memPart = getMemoryPart(partName, memoryClassName, partReader.getManifest());
        if (memPart != null) {
          memParts.put(partName, memPart);
        }
      }

      if (memParts.containsKey(partName)) {
        memParts.get(partName).addIteratorData(iterator.getKeyBytes(), iterator);
      }
    }
  }

  private MemoryIndexPart getMemoryPart(String partName, String memoryClassName, Parameters manifest) throws IOException {
    if (memoryClassName == null) {
      return null;
    }

    Class memoryIndexClass;
    try {
      memoryIndexClass = Class.forName(memoryClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + memoryClassName + ", which was specified as the memoryClass "
              + "in " + partName + ", could not be found.");
    }

    if (!MemoryIndexPart.class.isAssignableFrom(memoryIndexClass)) {
      throw new IOException(memoryClassName + " is not a MemoryIndexPart subclass.");
    }

    Constructor c;
    try {
      c = memoryIndexClass.getConstructor(Parameters.class);
    } catch (NoSuchMethodException ex) {
      throw new IOException(memoryClassName + " has no constructor that takes a single "
              + "Parameters argument.");
    } catch (SecurityException ex) {
      throw new IOException(memoryClassName + " doesn't have a suitable constructor that "
              + "this code has access to (SecurityException)");
    }

    MemoryIndexPart memoryPart;
    try {
      memoryPart = (MemoryIndexPart) c.newInstance(manifest);
    } catch (Exception ex) {
      IOException e = new IOException("Caught an exception while instantiating "
              + "a StructuredIndexPartReader: ");
      e.initCause(ex);
      throw e;
    }
    return memoryPart;
  }

  // Public functions specified by the Index interface
  // ALL of these functions are forwarded to the diskIndex
  @Override
  public String getDefaultPart() {
    return diskIndex.getDefaultPart();
  }

  @Override
  public String getIndexPartName(Node node) throws IOException {
    return diskIndex.getIndexPartName(node);
  }
  
  @Override
  public IndexPartReader getIndexPart(String name) throws IOException {
    return diskIndex.getIndexPart(name);
  }

  @Override
  public boolean containsPart(String partName) {
    return diskIndex.containsPart(partName);
  }

  @Override
  public boolean containsModifier(String partName, String modifierName) {
    return diskIndex.containsModifier(partName, modifierName);
  }

  @Override
  public void modify(ValueIterator iter, Node node) throws IOException {
    diskIndex.modify(iter, node);
  }

  @Override
  public NodeType getNodeType(Node node) throws Exception {
    return diskIndex.getNodeType(node);
  }

  @Override
  public CollectionStatistics getCollectionStatistics() {
    return diskIndex.getCollectionStatistics();
  }

  @Override
  public CollectionStatistics getCollectionStatistics(String part) {
    return diskIndex.getCollectionStatistics(part);
  }

  @Override
  public int getIdentifier(String document) throws IOException {
    return memNamesReader.getDocumentIdentifier(document);
  }

  @Override
  public Parameters getManifest() {
    return diskIndex.getManifest();
  }

  @Override
  public Set<String> getPartNames() {
    return diskIndex.getPartNames();
  }

  @Override
  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException {
    return diskIndex.getPartNodeTypes(partName);
  }
}
