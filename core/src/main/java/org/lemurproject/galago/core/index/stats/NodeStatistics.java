/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

import org.lemurproject.galago.utility.Parameters;

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
  public long computationCost = 0;

  public void add(NodeStatistics other) {
    nodeFrequency += other.nodeFrequency;
    nodeDocumentCount += other.nodeDocumentCount;
    maximumCount = (maximumCount < other.maximumCount) ? other.maximumCount : maximumCount;
  }

  public Parameters toParameters() {
    Parameters p = Parameters.create();
    p.set("node", this.node);
    p.set("nodeFrequency", this.nodeFrequency);
    p.set("maximumCount", this.maximumCount);
    p.set("nodeDocumentCount", this.nodeDocumentCount);
    return p;
  }

  public String toString() {
    return toParameters().toString();
  }

  @Override
  public NodeStatistics clone() {
    NodeStatistics ns = new NodeStatistics();
    ns.node = this.node;
    ns.nodeFrequency = this.nodeFrequency;
    ns.nodeDocumentCount = this.nodeDocumentCount;
    ns.maximumCount = this.maximumCount;
    return ns;
  }
}
