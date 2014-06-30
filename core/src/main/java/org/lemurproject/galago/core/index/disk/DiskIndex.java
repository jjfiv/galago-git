// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the main class for a disk based index structure
 *
 * A index is a set of parts Each part is a index file that offers
 * one or more iterators. Queries are be processed in this class using a tree of
 * iterators
 *
 * See the Index interface for the list of public functions.
 *
 * @author trevor, sjh, irmarc
 */
public class DiskIndex implements Index, Closeable {

  private static final Logger logger = Logger.getLogger("DiskIndex");
  protected File location;
  protected Parameters manifest = Parameters.instance();
  protected LengthsReader lengthsReader = null;
  protected NamesReader namesReader = null;
  protected NamesReverseReader namesReverseReader = null;
  protected Map<String, IndexPartReader> parts = new HashMap<String, IndexPartReader>();
  protected HashMap<String, String> defaultIndexOperators = new HashMap<String, String>();
  protected HashSet<String> knownIndexOperators = new HashSet<String>();

  // useful to assemble an index from odd pieces
  public DiskIndex(Collection<String> indexParts) throws IOException {
    location = null;

    for (String indexPart : indexParts) {
      File part = new File(indexPart);
      IndexComponentReader component = openIndexComponent(part.getAbsolutePath());
      initializeComponent(part.getName(), component);
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


    initializeIndexOperators();
  }

  public DiskIndex(File path) throws IOException {
    this(path.getAbsolutePath());
  }

  /**
   * recursively open index parts + infer if the file/folder is a part
   *
   * prefix should be empty string or a path ending with a slash
   */
  private void openDiskParts(String name, File directory) throws IOException {
    // check if the directory is a split index folder: (e.g. corpus)
    if (SplitBTreeReader.isBTree(directory)) {
      IndexComponentReader component = openIndexComponent(directory.getAbsolutePath());
      if (component != null) {
        initializeComponent(name, component);
      }
      return;
    }

    // otherwise the directory might contain stand-alone index files
    for (File part : FileUtility.safeListFiles(directory)) {
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

  private void initializeComponent(String name, IndexComponentReader component) {
    if (IndexPartReader.class.isAssignableFrom(component.getClass())) {
      parts.put(name, (IndexPartReader) component);
    }
  }

  @Override
  public String getIndexPath() {
    return location.getAbsolutePath();
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

  public static String getPartPath(String index, String part) {
    return (index + File.separator + part);
  }

  @Override
  public IndexPartReader getIndexPart(String part) throws IOException {
    if (parts.containsKey(part)) {
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
  public boolean containsDocumentIdentifier(long document) throws IOException {
    String name = this.namesReader.getDocumentName(document);
    return (name != null);
  }

  private void initializeIndexOperators() {
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
      if (parts.containsKey("postings.krovetz")) {
        this.defaultIndexOperators.put("counts", "postings.krovetz");
      } else if (parts.containsKey("postings.porter")) {
        this.defaultIndexOperators.put("counts", "postings.porter");
      } else if (parts.containsKey("postings")) {
        this.defaultIndexOperators.put("counts", "postings");
      }
    }
    if (!this.defaultIndexOperators.containsKey("extents")) {
      if (parts.containsKey("postings.krovetz")) {
        this.defaultIndexOperators.put("extents", "postings.krovetz");
      } else if (parts.containsKey("postings.porter")) {
        this.defaultIndexOperators.put("extents", "postings.porter");
      } else if (parts.containsKey("postings")) {
        this.defaultIndexOperators.put("extents", "postings");
      }
    }
    
    // Initialize these now b/c they're so common
    if (parts.containsKey("lengths")) {
      lengthsReader = (DiskLengthsReader) parts.get("lengths");
    } else {
      logger.log(Level.WARNING, "DiskIndex({0}) Index does not contain a lengths part.", location.getAbsolutePath());
    }
    if (parts.containsKey("names")) {
      namesReader = (DiskNameReader) parts.get("names");
    } else {
      logger.log(Level.WARNING, "DiskIndex({0}) Index does not contain a names part.", location.getAbsolutePath());
    }
    if (parts.containsKey("names.reverse")) {
      namesReverseReader = (DiskNameReverseReader) parts.get("names.reverse");
    } else {
      logger.log(Level.WARNING, "DiskIndex({0}) Index does not contain a names.reverse part.", location.getAbsolutePath());
    }
    if (parts.size() < 3) {
      logger.log(Level.WARNING, "DiskIndex({0}) Index contains fewer than 3 parts: this index might not be compatible with this version.", location.getAbsolutePath());
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
      final String operator = node.getOperator();
      final Map<String, NodeType> nodeTypes = part.getNodeTypes();
      result = nodeTypes.get(operator);
    }
    return result;
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    if (parts.containsKey(part)) {
      IndexPartReader p = parts.get(part);
      if (AggregateIndexPart.class.isInstance(p)) {
        return ((AggregateIndexPart) p).getStatistics();
      }
      throw new IllegalArgumentException("Index part, " + part + ", does not store aggregated statistics.");
    }
    throw new IllegalArgumentException("Index part, " + part + ", could not be found in index, " + this.location.getAbsolutePath());
  }

  @Override
  public void close() throws IOException {
    for (IndexPartReader part : parts.values()) {
      part.close();
    }
    parts.clear();
    lengthsReader.close();
    namesReader.close();
    namesReverseReader.close();
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
    return this.namesReverseReader.getDocumentIdentifier(document);
  }

  @Override
  public Document getDocument(String document, DocumentComponents p) throws IOException {
    if (parts.containsKey("corpus")) {
      try {
        CorpusReader corpus = (CorpusReader) parts.get("corpus");
        if(corpus == null) {
          throw new IllegalArgumentException("Attempted to pull a document from index without a corpus");
        }

        long docId = getIdentifier(document);
        return corpus.getDocument(docId, p);
      } catch (IOException e) {
        // ignore the exception
        logger.log(Level.SEVERE,"IOException while pulling document: "+document,e);
        /*logger.log(Level.SEVERE,
                "Failed to get document: {0}\n{1}",
                new Object[]{document, e.toString()});*/
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getDocuments(List<String> documents, DocumentComponents p) throws IOException {
    HashMap<String, Document> results = new HashMap<String,Document>();
		
		ArrayList<Long> docIds = new ArrayList<Long>();
    // should get a names iterator + sort requested documents
    for (String name : documents) {
			docIds.add(getIdentifier(name));
    }
		Collections.sort(docIds);
		
		CorpusReader corpus = (CorpusReader) parts.get("corpus");
    if(corpus == null) {
      throw new IllegalArgumentException("Attempted to pull documents from index without a corpus");
    }

    // loop over documents and pull them as requested
    CorpusReader.KeyIterator iter = corpus.getIterator();
		for (long id : docIds) {
			if (iter.findKey(Utility.fromLong(id))) {
				try {
					Document doc = iter.getDocument(p);
					if(doc != null) {
						results.put(doc.name, doc);
					}
				} catch (IOException e) {
					// ignore the exception
					Logger.getLogger(this.getClass().getName()).log(Level.SEVERE,
									"Failed to get document: {0}\n{1}",
									new Object[]{id, e.toString()});
				}
			}
		}
		
    return results;
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    return lengthsReader.getLengthsIterator();
  }

  @Override
  public DataIterator<String> getNamesIterator() throws IOException {
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
    IndexPartReader part = parts.get(partName);
    if (part == null) {
      throw new IllegalArgumentException("The index has no part named '" + partName + "'");
    }
    return part.getNodeTypes();
  }


  /* static functions for opening index component readers */
  public static IndexComponentReader openIndexComponent(String path) throws IOException {
    BTreeReader reader = BTreeFactory.getBTreeReader(path);

    // if it's not an index: return null
    if (reader == null) {
      return null;
    }

    // doesn't have a readerClass
    if (!reader.getManifest().isString("readerClass")) {

      // is this okay? e.g. DiskMapReader
      if(reader.getManifest().get("nonIndexPart", false))
        return null;

      // scream if somebody forgot to build a readerClass
      throw new IOException("Tried to open an index part at " + path + ", but the "
              + "file has no readerClass specified in its manifest. "
              + "(the readerClass is the class that knows how to decode the "
              + "contents of the file)");
    }

    String className = reader.getManifest().get("readerClass", (String) null);
    Class<?> readerClass;
    try {
      readerClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + ", which was specified as the readerClass in " + path + ", could not be found.");
    }

    if (!IndexComponentReader.class.isAssignableFrom(readerClass)) {
      throw new IOException(className + " is not a IndexComponentReader subclass.");
    }

    Constructor c;
    try {
      c = readerClass.getConstructor(BTreeReader.class);
    } catch (NoSuchMethodException ex) {
      throw new IOException(className + " has no constructor that takes a single IndexReader argument.");
    } catch (SecurityException ex) {
      throw new IOException(className + " doesn't have a suitable constructor that this code has access to (SecurityException)");
    }

    IndexComponentReader componentReader;
    try {
      componentReader = (IndexComponentReader) c.newInstance(reader);
    } catch (Exception ex) {
      throw new IOException("Caught an exception while instantiating a StructuredIndexPartReader: ", ex);
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

  @Override
  public String toString() {
    return "DiskIndex("+getIndexPath()+")";
  }
}
