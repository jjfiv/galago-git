// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.geometric;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.DynamicIndex;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.index.mem.FlushToDisk;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;


/*
 *  author: sjh, schiu
 *  
 *  Geometric Index uses an in-memory index
 *  periodically the memory index is flushed to disk
 *  depending on the number and size of the 
 *  indexes on disk, some index blocks may then be 
 *  merged 
 *  
 *  Notes:
 *  document ids are unique throughout the system
 *  merging process should no re-number documents
 *  
 *  indexBlockSize is the number of documents in 
 *  an index block empirically (over trec newswire 
 *  documents), 50000 documents should use between 
 *  500 and 800MB of RAM. Depending on your collection; 
 *  you will want to change this default.
 * 
 *  radix is the geometric parameter that determines
 *  the maximum number of index shards of any size.
 *  Once this number is reached, the set of shards 
 *  is merged.
 *  
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
public class GeometricIndex implements DynamicIndex, Index {

  // indexing statics 
  private static final Logger logger = Logger.getLogger(GeometricIndex.class.toString());
  private TupleFlowParameters tupleFlowParameters;
  private Parameters globalParameters;
  private String shardDirectory;
  private int indexBlockSize; // measured in documents
  // indexing dynamics
  private MemoryIndex currentMemoryIndex;
  private GeometricPartitions geometricParts;
  private int indexBlockCount;
  public long globalDocumentCount;
  // checkpoint data
  private CheckPointHandler checkpointer;
  private String lastAddedDocumentIdentifier = "";
  private long lastAddedDocumentNumber = -1;

  public GeometricIndex(TupleFlowParameters parameters) throws Exception {
    this(parameters, new CheckPointHandler());
  }

  public GeometricIndex(TupleFlowParameters parameters, CheckPointHandler checkpointer) throws Exception {
    this.tupleFlowParameters = parameters;
    this.globalParameters = parameters.getJSON();

    this.shardDirectory = this.globalParameters.getString("shardDirectory");
    this.indexBlockSize = (int) this.globalParameters.get("indexBlockSize", 1000);

    // radix is the number of indexes of each size to store before a merge op
    // keep in mind that the total number of indexes is difficult to control
    int radix = (int) globalParameters.get("radix", 3);
    this.geometricParts = new GeometricPartitions(radix);

    // initialisation
    this.globalDocumentCount = 0;
    this.indexBlockCount = 0;

    // checkpoint handler
    this.checkpointer = checkpointer;
    this.checkpointer.setDirectory(this.shardDirectory);
    if (globalParameters.get("resumable", false)) {
      restoreToCheckpoint(checkpointer.getRestore());
    }

    resetCurrentMemoryIndex();
    updateIndex();
  }

  public void process(Document doc) throws IOException {
    currentMemoryIndex.process(doc);
    globalDocumentCount++; // now one higher than the document just processed

    lastAddedDocumentIdentifier = doc.name;
    lastAddedDocumentNumber = globalDocumentCount;

    if (globalDocumentCount % indexBlockSize == 0) {
      flushCurrentIndexBlock();
      maintainMergeLocal();
    }
  }

  @Override
  public void close() throws IOException {
    // this will ensure that all data is on disk
    flushCurrentIndexBlock();

    // logger.info("Performing final merge");
    // try {
    //Bin finalMergeBin = geometricParts.getAllShards();
    //doMerge(finalMergeBin, getNextIndexShardFolder(finalMergeBin.size + 1));
    // check point is updated by the merge op.

    //  } catch (IOException ex) {
    //  Logger.getLogger(GeometricRetrieval.class.getName()).log(Level.SEVERE, null, ex);
    //}

  }

  // tries to flush memory index
  public void forceFlush() throws IOException {
    flushCurrentIndexBlock();
  }

  /**
   * Tries to perform a merge op (for use with force flush) NOTE: flush will
   * only occur when geometric merge requirements are true - more than <radix>
   * shards of a given size
   *
   */
  public void forceMerge() throws IOException {
    maintainMergeLocal();
  }

  // some public functions can be defered to the MemoryIndex (disk indexes are identical in structure).
  public String getDefaultPart() {
    return this.currentMemoryIndex.getDefaultPart();
  }

  public String getIndexPartName(Node node) throws IOException {
    return this.currentMemoryIndex.getIndexPartName(node);
  }

  @Override
  public IndexPartReader getIndexPart(String part) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public boolean containsPart(String partName) {
    return this.currentMemoryIndex.containsPart(partName);
  }

  public NodeType getNodeType(Node node) throws Exception {
    return this.currentMemoryIndex.getNodeType(node);
  }

  public Set<String> getPartNames() {
    return this.currentMemoryIndex.getPartNames();
  }

  public Map<String, NodeType> getPartNodeTypes(String partName) throws IOException {
    return this.currentMemoryIndex.getPartNodeTypes(partName);
  }

  public boolean containsModifier(String partName, String modifierName) {
    return this.currentMemoryIndex.containsModifier(partName, modifierName);
  }

  public Parameters getManifest() {
    return this.globalParameters;
  }

  public boolean containsDocumentIdentifier(int document) throws IOException {
    if (currentMemoryIndex.containsDocumentIdentifier(document)) {
      return true;
    }
    for (DiskIndex i : this.geometricParts.getIndexes()) {
      if (i.containsDocumentIdentifier(document)) {
        return true;
      }
    }
    return false;
  }

  public BaseIterator getIterator(Node node) throws IOException {
    List<BaseIterator> itrs = new ArrayList();
    itrs.add(this.currentMemoryIndex.getIterator(node));
    for (DiskIndex di : this.geometricParts.getIndexes()) {
      BaseIterator vi = di.getIterator(node);
      if (vi != null) {
        itrs.add(di.getIterator(node));
      }
    }
    if (itrs.size() > 0) {
      if (node.getOperator().equals("counts")) {
        return new DisjointCountsIterator((Collection) itrs);
      } else if (node.getOperator().equals("extents")) {
        return new DisjointLengthsIterator((Collection) itrs);
      } else if (node.getOperator().equals("lengths")) {
        return new DisjointLengthsIterator((Collection) itrs);
      } else if (node.getOperator().equals("names")) {
        return new DisjointNamesIterator((Collection) itrs);
      }
      // TODO: add other supported iterator classes as required.
    }
    return null;
  }

  // Note: this data is correct only at time of requesting.
  // DO NOT CACHE THIS DATA.
  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    IndexPartStatistics stats = this.currentMemoryIndex.getIndexPartStatistics(part);
    for (DiskIndex di : this.geometricParts.getIndexes()) {
      stats.add(di.getIndexPartStatistics(part));
    }
    // fix the part name
    stats.partName = part;
    return stats;
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsIterator i = (LengthsIterator) this.getLengthsIterator();
    i.syncTo(document);
    if (i.hasMatch(document)) {
      return i.length();
    } else {
      throw new IOException("Could not find document identifier " + document);
    }
  }

  @Override
  public String getName(int document) throws IOException {
    NamesReader.NamesIterator i = this.getNamesIterator();
    i.syncTo(document);
    if (i.hasMatch(document)) {
      return i.getCurrentName();
    } else {
      throw new IOException("Could not find document identifier " + document);
    }
  }

  public int getIdentifier(String document) throws IOException {
    throw new RuntimeException("UNIMPLEMENTED function: getIdentifier");
  }

  @Override
  public Document getDocument(String document, DocumentComponents p) throws IOException {
    throw new RuntimeException("UNIMPLEMENTED function: getdocument");
  }

  @Override
  public Map<String, Document> getDocuments(List<String> documents, DocumentComponents p) throws IOException {
    throw new RuntimeException("UNIMPLEMENTED function: getdocuments");
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    List<LengthsIterator> itrs = new ArrayList();
    itrs.add(currentMemoryIndex.getLengthsIterator());
    for (DiskIndex di : this.geometricParts.getIndexes()) {
      itrs.add(di.getLengthsIterator());
    }
    return new DisjointLengthsIterator(itrs);
  }

  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    List<NamesReader.NamesIterator> itrs = new ArrayList();
    itrs.add(currentMemoryIndex.getNamesIterator());
    for (DiskIndex di : this.geometricParts.getIndexes()) {
      itrs.add(di.getNamesIterator());
    }
    return new DisjointNamesIterator(itrs);
  }

  public void modify(DiskIterator iter, Node node) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  // private and internal functions

  /*
   * This function is called after each index flush
   *  and after each index merge operation
   * 
   * It ensures the set of retrievals are updated to reflect the flush/merge op
   *  and the collection statistics used for retrieval are maintained correctly.
   * 
   */
  private void updateIndex() throws IOException {
    // maintain the document store (corpus) - if there is one
    if (currentMemoryIndex.containsPart("corpus")) {
      // get all corpora + shove into document store
      ArrayList<DocumentReader> readers = new ArrayList<DocumentReader>();
      readers.add((DocumentReader) currentMemoryIndex.getIndexPart("corpus"));
      for (String path : geometricParts.getAllShards().getBinPaths()) {
        String corpus = path + File.separator + "corpus";
        readers.add(new CorpusReader(corpus));
      }
    }
    // finally write new checkpointing data (checkpoints the disk indexes)
    Parameters checkpoint = createCheckpoint();
    this.checkpointer.saveCheckpoint(checkpoint);
  }

  // handles the memory index
  private void resetCurrentMemoryIndex() throws Exception {
    // by using the globalParameters, the memory index can startup counters etc.
    // we set the documentCount to ensure all documents are given a unique number.
    tupleFlowParameters.getJSON().set("documentNumberOffset", this.globalDocumentCount);
    currentMemoryIndex = new MemoryIndex(tupleFlowParameters);
  }

  private void flushCurrentIndexBlock() throws IOException {
    // First check that the final memory index contains some data:
    if (currentMemoryIndex.documentsInIndex() < 1) {
      return;
    }

    logger.info("Flushing current memory Index. id = " + indexBlockCount);

    final GeometricIndex g = this;
    final MemoryIndex flushingMemoryIndex = currentMemoryIndex;
    final File shardFolder = getNextIndexShardFolder(1);

    try {
      // reset the current index
      //  - this makes the flush operation thread safe while continuing to add new documents.
      resetCurrentMemoryIndex();
    } catch (Exception ex) {
      throw new IOException(ex);
    }

    try {
      // first flush the index to disk
      (new FlushToDisk()).flushMemoryIndex(flushingMemoryIndex, shardFolder.getAbsolutePath(), false);

      // indicate that the flushing part of this thread is done
      synchronized (geometricParts) {
        // add flushed index to the set of bins -- needs to be a synconeous action
        geometricParts.add(0, shardFolder.getAbsolutePath());
        updateIndex();
        flushingMemoryIndex.close();
      }

    } catch (IOException e) {
      logger.severe(e.toString());
    }
  }

  // handle the on-disk index merging operations
  private void maintainMergeLocal() {
    logger.info("Maintaining Merge Local");
    try {
      Bin mergeBin = null;
      synchronized (geometricParts) {
        mergeBin = geometricParts.findMergeCandidates();
      }
      if (!mergeBin.isEmpty()) {
        File indexShard = getNextIndexShardFolder(mergeBin.size + 1);
        // otherwise there's something to merge
        logger.info("Performing merge!");


        // merge the shards
        Parameters p = this.globalParameters.clone();
        // override each of these particular parameters
        p.set("indexPath", indexShard.getAbsolutePath());
        p.set("inputPath", new ArrayList(mergeBin.getBinPaths()));
        p.set("renumberDocuments", false);

        App.run("merge-index", p, System.out);

        // should make sure that these two are uninteruppable
        synchronized (geometricParts) {
          geometricParts.add(mergeBin.size + 1, indexShard.getAbsolutePath());
          geometricParts.removeShards(mergeBin);
          updateIndex();
        }

        // now can delete the merged indexshard folders...
        for (String file : mergeBin.getBinPaths()) {
          Utility.deleteDirectory(new File(file));
        }

        logger.info("Done merging.");
      }
    } catch (Exception ex) {
      Logger.getLogger(GeometricIndex.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private File getNextIndexShardFolder(int size) {
    File indexFolder = new File(shardDirectory + File.separator + "galagoindex.shard." + indexBlockCount + "." + size);
    indexFolder.mkdirs();
    indexBlockCount++;

    return indexFolder;
  }

  // should contain parameters that can be used to restart the geometric index
  // should also contain the last document name that was written to disk
  // (copied from the last flush)
  private Parameters createCheckpoint() {
    Parameters checkpoint = new Parameters();
    checkpoint.set("lastDoc/identifier", this.lastAddedDocumentIdentifier);
    checkpoint.set("lastDoc/number", this.lastAddedDocumentNumber);
    checkpoint.set("indexBlockCount", this.indexBlockCount);
    Parameters shards = new Parameters();
    for (Bin b : this.geometricParts.radixBins.values()) {
      for (String indexPath : b.getBinPaths()) {
        shards.set(indexPath, b.size);
      }
    }
    checkpoint.set("shards", shards);
    return checkpoint;
  }

  // should only be called at startup
  private void restoreToCheckpoint(Parameters checkpoint) {
    assert geometricParts.diskShardCount() == 0 : "Restore to Checkpoint should only be called at startup!";

    this.lastAddedDocumentIdentifier = checkpoint.getString("lastDoc/identifier");
    this.lastAddedDocumentNumber = (int) checkpoint.getLong("lastDoc/number");
    this.indexBlockCount = (int) checkpoint.getLong("indexBlockCount");
    Parameters shards = checkpoint.getMap("shards");
    for (String indexPath : shards.getKeys()) {
      this.geometricParts.add((int) shards.getLong(indexPath), indexPath);
    }
  }

  // Subclasses
  private class Bin {

    private int size;
    private HashSet<String> binPaths = new HashSet<String>();

    public Bin(int size) {
      this.size = size;
    }

    public void add(Bin b) {
      binPaths.addAll(b.binPaths);
    }

    public void add(String path) {
      binPaths.add(path);
    }

    public void removeAll(Bin b) {
      binPaths.removeAll(b.binPaths);
    }

    public int count() {
      return binPaths.size();
    }

    public Collection<String> getBinPaths() {
      return binPaths;
    }

    public boolean isEmpty() {
      return binPaths.isEmpty();
    }
  }

  private class GeometricPartitions {

    private int radix;
    private TreeMap<Integer, Bin> radixBins = new TreeMap();
    private TreeMap<String, DiskIndex> activeIndexes = new TreeMap();

    public GeometricPartitions(int radix) {
      this.radix = radix;
    }

    public Bin get(int size) {
      return radixBins.get(new Integer(size));
    }

    public Collection<DiskIndex> getIndexes() {
      return this.activeIndexes.values();
    }

    public void add(int size, String path) {
      try {
        DiskIndex index = new DiskIndex(path);
        activeIndexes.put(path, index);
      } catch (IOException e) {
        logger.severe("Index " + path + " could not be opened. Index will be ignored - not deleted.");
        return;
      }

      if (!radixBins.containsKey(size)) {
        radixBins.put(size, new Bin(size));
      }
      radixBins.get(size).add(path);
    }

    // Specifically returns a new Bin with the set of file paths
    // this means that computation can continue as required.
    //
    // If cascade is true, then this will also add larger bin sizes
    // if merging the current bin size will cause the next one to
    // reach radix.
    public Bin findMergeCandidates() {
      Bin candidate;
      Bin result = new Bin(0);
      for (int i = 0; i <= getMaxSize(); i++) {
        candidate = radixBins.get(i);
        if (candidate.count() + ((candidate.size == i) ? 0 : 1) >= radix) {
          logger.info("Adding Merge Candidate of size: " + i);
          result.size = i;
          result.add(candidate);
        } else {
          break;
        }
      }
      return result;
    }

    public Bin getAllShards() {
      Bin result = new Bin(0);
      result.add(shardDirectory);
      for (Integer i : radixBins.keySet()) {
        if (i.intValue() > result.size) {
          result.size = i.intValue();
        }
        result.add(radixBins.get(i));
      }
      return result;
    }

    // only remove merged shards
    public void removeShards(Bin shards) {
      for (String path : shards.getBinPaths()) {
        activeIndexes.remove(path);
      }
      //search all bins and remove.
      for (Integer i : radixBins.keySet()) {
        radixBins.get(i).removeAll(shards);
      }
    }

    public int getMaxSize() {
      int max = 0;
      for (int i : radixBins.keySet()) {
        if (i > max) {
          max = i;
        }
      }
      return max;
    }

    public int diskShardCount() {
      int count = 0;
      for (Bin b : radixBins.values()) {
        count += b.binPaths.size();
      }
      return count;
    }
  }
}
