/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class IndexPartStatistics implements AggregateStatistic {

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
    Parameters p = Parameters.create();
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

  public IndexPartStatistics clone() {
    IndexPartStatistics ps = new IndexPartStatistics();
    ps.partName = this.partName;
    ps.collectionLength = this.collectionLength;
    ps.vocabCount = this.vocabCount;
    ps.highestDocumentCount = this.highestDocumentCount;
    ps.highestFrequency = this.highestFrequency;
    return ps;
  }
}
