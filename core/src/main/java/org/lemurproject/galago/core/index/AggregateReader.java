// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.Serializable;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Class holds
 *
 * @author sjh
 */
public class AggregateReader {

  public static class IndexPartStatistics implements Serializable {

    private static final long serialVersionUID = 5553653651892088433L;
    public String partName = null;
    public long collectionLength = 0;
    public long vocabCount = 0;
    public long highestDocumentCount = 0;
    public long highestFrequency = 0;

    public IndexPartStatistics() {
    }

    public IndexPartStatistics(String partName, Parameters parameters) {
      this.partName = partName;
      this.collectionLength = parameters.get("statistics/collectionLength", 0L);
      this.vocabCount = parameters.get("statistics/vocabCount", 0L);
      this.highestDocumentCount = parameters.get("statistics/highestDocumentCount", 0L);
      this.highestFrequency = parameters.get("statistics/highestFrequency", 0L);
    }

    public void add(IndexPartStatistics other) {
      this.collectionLength += other.collectionLength;
      this.vocabCount = Math.max(this.vocabCount, other.vocabCount);
      this.highestDocumentCount = Math.max(this.highestDocumentCount, other.highestDocumentCount);
      this.highestFrequency = Math.max(this.highestFrequency, other.highestFrequency);
    }

    public Parameters toParameters() {
      Parameters p = new Parameters();
      p.set("partName", partName);
      p.set("statistics/collectionLength", collectionLength);
      p.set("statistics/vocabCount", vocabCount);
      p.set("statistics/highestDocumentCount", highestDocumentCount);
      p.set("statistics/highestFrequency", highestFrequency);
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
  public static class CollectionStatistics implements Serializable {

    private static final long serialVersionUID = 6553653651892088433L;
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
    public long nonZeroLenDocCount = 0;

    public CollectionStatistics() {
    }

    public void add(CollectionStatistics other) {
      this.collectionLength += other.collectionLength;
      this.documentCount += other.documentCount;
      this.nonZeroLenDocCount += other.nonZeroLenDocCount;
      this.maxLength = Math.max(this.maxLength, other.maxLength);
      this.minLength = Math.min(this.minLength, other.minLength);
      this.avgLength = (this.documentCount > 0) ? this.collectionLength / this.documentCount : -1;
    }

    public Parameters toParameters() {
      Parameters p = new Parameters();
      p.set("fieldName", this.fieldName);
      p.set("collectionLength", this.collectionLength);
      p.set("documentCount", this.documentCount);
      p.set("nonZeroLenDocCount", this.nonZeroLenDocCount);
      p.set("maxLength", this.maxLength);
      p.set("minLength", this.minLength);
      p.set("avgLength", this.avgLength);
      return p;
    }

    public String toString() {
      return toParameters().toString();
    }
  }

  public static class NodeStatistics implements Serializable {

    private static final long serialVersionUID = 7553653651892088433L;
    public String node = null;
    public long nodeFrequency = 0;
    public long nodeDocumentCount = 0;
    public long maximumCount = 0;

    public void add(NodeStatistics other) {
      nodeFrequency += other.nodeFrequency;
      nodeDocumentCount += other.nodeDocumentCount;
    }

    public Parameters toParameters() {
      Parameters p = new Parameters();
      p.set("fieldName", this.node);
      p.set("nodeFrequency", this.nodeFrequency);
      p.set("maximumCount", this.maximumCount);
      p.set("nodeDocumentCount", this.nodeDocumentCount);
      return p;
    }

    public String toString() {
      return toParameters().toString();
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
