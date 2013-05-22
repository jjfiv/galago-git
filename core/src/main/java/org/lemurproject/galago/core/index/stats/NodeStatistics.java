/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class NodeStatistics implements AggregateStatistic {

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
    p.set("node", this.node);
    p.set("nodeFrequency", this.nodeFrequency);
    p.set("maximumCount", this.maximumCount);
    p.set("nodeDocumentCount", this.nodeDocumentCount);
    return p;
  }

  public String toString() {
    return toParameters().toString();
  }
}
