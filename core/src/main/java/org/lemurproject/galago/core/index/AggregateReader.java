// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Class holds
 *
 * @author sjh
 */
public class AggregateReader {

  public static class IndexPartStatistics {

    public String partName = null;
    public long collectionLength = 0;
    public long vocabCount = 0;
    public long highestDocumentCount = 0;
    public long highestFrequency = 0;

    public IndexPartStatistics(){
    }
    
    public IndexPartStatistics(String partName, Parameters parameters) {
      this.partName = partName;
      collectionLength = parameters.get("statistics/collectionLength", 0L);
      vocabCount = parameters.get("statistics/vocabCount", 0L);
    }

    public IndexPartStatistics(String partName, MemoryIndex index) {
      this.partName = partName;
      collectionLength = index.getIndexPart(partName).getCollectionLength();
      vocabCount = index.getIndexPart(partName).getKeyCount();
    }

    public void add(IndexPartStatistics other) {
      assert this.partName.equals(other.partName);
      this.collectionLength += other.collectionLength;
      this.vocabCount += other.vocabCount;
    }

    public Parameters toParameters() {
      Parameters p = new Parameters();
      p.set("statistics/collectionLength", collectionLength);
      p.set("statistics/vocabCount", vocabCount);
      return p;
    }

    public String toString() {
      return toParameters().toString();
    }
  }

  /**
   * Collection Statistics. Stores aggregate values used for smoothing models of
   * documents.
   */
  public static class CollectionStatistics {

    // 'document', 'field', or passage label
    public String fieldName = null;
    // total number of terms in field in the collection
    public long collectionLength = 0;
    // total number of documents that contain field
    public long documentCount = 0;
    // note for the next three values: 
    //  - (instances of 'field' in a document are summed together)
    // maximum length of 'field'
    public long maxLength = 0;
    // minimum length of 'field'
    public long minLength = 0;
    // average length of 'field' 
    public double avgLength = 0;

    public CollectionStatistics() {
    }

    public CollectionStatistics(Parameters p) {
      this.fieldName = p.getString("fieldName");
      this.collectionLength = p.getLong("collectionLength");
      this.documentCount = p.getLong("documentCount");
      this.maxLength = p.getLong("maxLength");
      this.minLength = p.getLong("minLength");
      this.avgLength = p.getDouble("avgLength");
    }

    public void add(CollectionStatistics other) {
      assert (this.fieldName.equals(other.fieldName)) : "";
      this.collectionLength += other.collectionLength;
      this.documentCount += other.documentCount;
      this.maxLength = Math.max(this.maxLength, other.maxLength);
      this.minLength = Math.min(this.minLength, other.minLength);
      this.avgLength = (this.documentCount > 0) ? this.collectionLength / this.documentCount : -1;
    }

    public Parameters toParameters() {
      Parameters p = new Parameters();
      p.set("fieldName", this.fieldName);
      p.set("collectionLength", this.collectionLength);
      p.set("documentCount", this.documentCount);
      p.set("maxLength", this.maxLength);
      p.set("minLength", this.minLength);
      p.set("avgLength", this.avgLength);
      return p;
    }
  }

  public static class NodeStatistics {

    public String node = null;
    public long nodeFrequency = 0;
    public long nodeDocumentCount = 0;
    public long maximumCount = 0;

    public String toString() {
      return "{ \"node\" : \"" + node + "\","
              + "\"nodeFrequency\" : " + nodeFrequency + ","
              + "\"maximumCount\" : " + maximumCount + ","
              + "\"nodeDocumentCount\" : " + nodeDocumentCount + "}";
    }

    public void add(NodeStatistics other) {
      // assert this.node.equals(other.node); // doesn't work.. need to investigate why.
      nodeFrequency += other.nodeFrequency;
      maximumCount = Math.max(this.maximumCount, other.maximumCount);
      nodeDocumentCount += other.nodeDocumentCount;
    }
  }

  public static interface AggregateIndexPart {

    public IndexPartStatistics getStatistics();
  }

  public static interface CollectionAggregateIterator {

    public CollectionStatistics getStatistics();
  }

  public static interface NodeAggregateIterator {

    public NodeStatistics getStatistics();
  }
}
