/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class FieldStatistics implements AggregateStatistic {

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

    public FieldStatistics() {
    }

    public void add(FieldStatistics other) {
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
