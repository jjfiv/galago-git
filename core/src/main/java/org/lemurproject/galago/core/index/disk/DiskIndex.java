// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.util.logging.Level;
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.index.IndexPartModifier;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This is the main class for a disk based index structure
 * 
 * A index is a set of parts and modifiers
 * Each part is a index file that offers one or more iterators
 * Each modifier alters or extends the data provided by the corresponding part
 * Queries are be processed in this class using a tree of iterators
 * 
 * See the Index interface for the list of public functions.
 * 
 * @author trevor, sjh, irmarc
 */
public class DiskIndex implements Index {

  protected File location;
  protected Parameters manifest = new Parameters();
  protected LengthsReader lengthsReader = null;
  protected NamesReader namesReader = null;
  
  protected Map<String, IndexPartReader> parts = new HashMap<String, IndexPartReader>();
  protected Map<String, CollectionStatistics> partStatistics = new HashMap<String, CollectionStatistics>();
  protected Map<String, HashMap<String, IndexPartModifier>> modifiers = new HashMap<String, HashMap<String, IndexPartModifier>>();
  protected HashMap<String, String> defaultIndexOperators = new HashMap<String, String>();
  protected HashSet<String> knownIndexOperators = new HashSet<String>();

  // useful to assemble an index from odd pieces
  public DiskIndex(Collection<String> indexParts) throws IOException{
    location = null;

    for(String indexPart : indexParts){
      File part = new File(indexPart);
      IndexComponentReader component = openIndexComponent(part.getAbsolutePath());
      initializeComponent(part.getName(), component);
    }
    // Initialize these now b/c they're so common
    if (parts.containsKey("lengths")) {
      lengthsReader = (DiskLengthsReader) parts.get("lengths");
    }
    if (parts.containsKey("names")) {
      namesReader = (DiskNameReader) parts.get("names");
    }

    initializeIndexOperators();
  }


  public DiskIndex(String indexPath) throws IOException {
    // Make sure it's a valid location    
    location = new File(indexPath);
    if (!location.isDirectory()) {
      throw new IOException(String.format("%s is not a directory.", indexPath));
    }

    // Load all parts
    openDiskParts("", location);

    // Initialize these now b/c they're so common
    if (parts.containsKey("lengths")) {
      lengthsReader = (DiskLengthsReader) parts.get("lengths");
    }
    if (parts.containsKey("names")) {
      namesReader = (DiskNameReader) parts.get("names");
    }

    initializeIndexOperators();
  }

  /**
   * recursively open index parts
   *  + infer if the file/folder is a part or a modifier
   * 
   *  prefix should be empty string or a path ending with a slash
   */
  protected void openDiskParts(String name, File directory) throws IOException {
    // check if the directory is a split index folder: (e.g. corpus)
    if (SplitBTreeReader.isBTree(directory)) {
      IndexComponentReader component = openIndexComponent(directory.getAbsolutePath());
      if (component != null) {
        initializeComponent(name, component);
      }
      return;
    }

    // otherwise the directory might contain stand-alone index files
    for (File part : directory.listFiles()) {
      String partName = (name.length() == 0) ? part.getName() : name + "/" + part.getName();
      if (part.isDirectory()) {
        openDiskParts(partName, part);
      } else {
        IndexComponentReader component = openIndexComponent(part.getAbsolutePath());
        if (component != null) {
          initializeComponent(partName, component);
        }
      }
    }
  }

  protected void initializeComponent(String name, IndexComponentReader component) {
    if (IndexPartReader.class.isAssignableFrom(component.getClass())) {
      parts.put(name, (IndexPartReader) component);
      partStatistics.put(name, new CollectionStatistics(name, component.getManifest()));

    } else if (IndexPartModifier.class.isAssignableFrom(component.getClass())) {
      // need to pop off the dirname : (e.g. mod/)
      String[] nameParts = name.split("\\.");

      if (nameParts.length >= 2) {
        if (!modifiers.containsKey(nameParts[0])) {
          modifiers.put(nameParts[0], new HashMap<String, IndexPartModifier>());
        }
        modifiers.get(nameParts[0]).put(nameParts[1], (IndexPartModifier) component);
      }
    }
  }

  public File getIndexLocation() {
    return location;
  }

  // I'd really prefer to get this from the manifest, but writing the manifest reliably seems
  // difficult right now, so just use it if available, otherwise use well-known defaults.
  @Override
  public String getDefaultPart() {
    if (manifest.containsKey("defaultPart")) {
      String part = manifest.getString("defaultPart");
      if (parts.containsKey(part)) {
        return part;
      }
    }

    // otherwise, try to default
    if (parts.containsKey("postings.porter")) {
      return "postings.porter";
    }
    if (parts.containsKey("postings.krovetz")) {
      return "postings.krovetz";
    }
    if (parts.containsKey("postings")) {
      return "postings";
    }
    // otherwise - anything will do.
    return parts.keySet().iterator().next();
  }

  public static String getPartPath(String index, String part) {
    return (index + File.separator + part);
  }

  @Override
  public IndexPartReader getIndexPart(String part) throws IOException {
    if(parts.containsKey(part)){
      return parts.get(part);
    } else {
      return null;
    }
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
  public boolean containsModifier(String partName, String modifierName) {
    return (modifiers.containsKey(partName)
            && modifiers.get(partName).containsKey(modifierName));
  }

  @Override
  public boolean containsDocumentIdentifier(int document) throws IOException {
    NamesReader.Iterator ni = this.getNamesIterator();
    ni.moveTo(document);
    return ni.hasMatch(document);
  }

  protected void initializeIndexOperators() {
    for (Entry<String, IndexPartReader> entry : parts.entrySet()) {
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

    // HACK - for now //
    if (!this.defaultIndexOperators.containsKey("counts")) {
      if (parts.containsKey("postings.porter")) {
        this.defaultIndexOperators.put("counts", "postings.porter");
      } else if (parts.containsKey("postings")) {
        this.defaultIndexOperators.put("counts", "postings");
      }
    }
    if (!this.defaultIndexOperators.containsKey("extents")) {
      if (parts.containsKey("postings.porter")) {
        this.defaultIndexOperators.put("extents", "postings.porter");
      } else if (parts.containsKey("postings")) {
        this.defaultIndexOperators.put("extents", "postings");
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

  /**
   * Modifies the constructed iterator to contain any modifications
   * requested, if they are found.
   * 
   * @param iter
   * @param node
   * @throws IOException
   */
  @Override
  public void modify(ValueIterator iter, Node node) throws IOException {
    if (ModifiableIterator.class.isInstance(iter)) {
      NodeParameters p = node.getNodeParameters();
      if (modifiers.containsKey(p.get("part", "none"))) {
        HashMap<String, IndexPartModifier> partModifiers = modifiers.get(p.getString("part"));
        if (partModifiers.containsKey(p.get("mod", "none"))) {
          IndexPartModifier modder = partModifiers.get(p.getString("mod"));
          Object modification = modder.getModification(node);
          if (modification != null) {
            ((KeyListReader.ListIterator) iter).addModifier(p.getString("mod"), modification);
          }
        }
      }
    }
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    ValueIterator result = null;
    IndexPartReader part = parts.get(getIndexPartName(node));
    if (part != null) {
      result = part.getIterator(node);
      modify(result, node);
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
      final String operator = node.getOperator();
      final Map<String, NodeType> nodeTypes = part.getNodeTypes();
      result = nodeTypes.get(operator);
    }
    return result;
  }

  @Override
  public CollectionStatistics getCollectionStatistics() {
    return getCollectionStatistics(this.getDefaultPart());
  }

  @Override
  public CollectionStatistics getCollectionStatistics(String part) {
    return this.partStatistics.get(part);
  }

  @Override
  public void close() throws IOException {
    for (IndexPartReader part : parts.values()) {
      part.close();
    }
    parts.clear();
    lengthsReader.close();
    namesReader.close();
  }

  @Override
  public int getLength(int document) throws IOException {
    return lengthsReader.getLength(document);
  }

  @Override
  public String getName(int document) throws IOException {
    return namesReader.getDocumentName(document);
  }

  @Override
  public int getIdentifier(String document) throws IOException {
    return ((NamesReader) parts.get("names.reverse")).getDocumentIdentifier(document);
  }

  @Override
  public Document getDocument(String document, Parameters p) throws IOException {
    if (parts.containsKey("corpus")) {
      try {
        CorpusReader corpus = (CorpusReader) parts.get("corpus");
        int docId = getIdentifier(document);
        return corpus.getDocument(docId,  p);
      } catch (Exception e) {
        // ignore the exception
        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, 
							"Failed to get document: {0}\n{1}", 
							new Object[]{document, e.toString()});
      }
    }
    return null;
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

  @Override
  public LengthsReader.Iterator getLengthsIterator() throws IOException {
    return lengthsReader.getLengthsIterator();
  }

  @Override
  public NamesReader.Iterator getNamesIterator() throws IOException {
    return namesReader.getNamesIterator();
  }

  @Override
  public Parameters getManifest() {
    return manifest.clone();
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


  /* static functions for opening index component readers */
  public static IndexComponentReader openIndexComponent(String path) throws IOException {
    BTreeReader reader = BTreeFactory.getBTreeReader(path);

    // if it's not an index: return null
    if (reader == null) {
      return null;
    }

    if (!reader.getManifest().isString("readerClass")) {
      throw new IOException("Tried to open an index part at " + path + ", but the "
              + "file has no readerClass specified in its manifest. "
              + "(the readerClass is the class that knows how to decode the "
              + "contents of the file)");
    }

    String className = reader.getManifest().get("readerClass", (String) null);
    Class readerClass;
    try {
      readerClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + ", which was specified as the readerClass "
              + "in " + path + ", could not be found.");
    }

    if (!IndexComponentReader.class.isAssignableFrom(readerClass)) {
      throw new IOException(className + " is not a IndexComponentReader subclass.");
    }

    Constructor c;
    try {
      c = readerClass.getConstructor(BTreeReader.class);
    } catch (NoSuchMethodException ex) {
      throw new IOException(className + " has no constructor that takes a single "
              + "IndexReader argument.");
    } catch (SecurityException ex) {
      throw new IOException(className + " doesn't have a suitable constructor that "
              + "this code has access to (SecurityException)");
    }

    IndexComponentReader componentReader;
    try {
      componentReader = (IndexComponentReader) c.newInstance(reader);
    } catch (Exception ex) {
      IOException e = new IOException("Caught an exception while instantiating "
              + "a StructuredIndexPartReader: ");
      e.initCause(ex);
      throw e;
    }
    return componentReader;
  }

  public static IndexPartReader openIndexPart(String path) throws IOException {
    IndexComponentReader componentReader = openIndexComponent(path);
    if (!IndexPartReader.class.isAssignableFrom(componentReader.getClass())) {
      throw new IOException(componentReader.getClass().getName() + " is not a IndexPartReader subclass.");
    }
    return (IndexPartReader) componentReader;
  }

  public static IndexPartModifier openIndexModifier(String path) throws IOException {
    IndexComponentReader componentReader = openIndexComponent(path);
    if (!IndexPartModifier.class.isAssignableFrom(componentReader.getClass())) {
      throw new IOException(componentReader.getClass().getName() + " is not a IndexPartModifier subclass.");
    }
    return (IndexPartModifier) componentReader;
  }
}
