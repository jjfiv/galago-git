// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.lists;

import org.lemurproject.galago.utility.CmpUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author jfoley
 */
public abstract class Ranked extends Scored implements Serializable {
  private static final long serialVersionUID = -6063499164129611150L;
  public int rank;
  public Ranked(int rank, double score) {
    super(score);
    this.rank = rank;
  }

  public static <T extends Ranked> void setRanksByScore(List<T> rankedList) {
    Collections.sort(rankedList, byDescendingScore);
    for (int i = 0; i < rankedList.size(); i++) {
      rankedList.get(i).rank = i+1;
    }
  }

  public double reciprocalRank() {
    return 1.0 / rank;
  }

  public static Comparator<Ranked> byDescendingScore = new Comparator<Ranked>() {
    @Override
    public int compare(Ranked lhs, Ranked rhs) {
      return -CmpUtil.compare(lhs.score, rhs.score);
    }
  };

  public static Comparator<Ranked> byAscendingRank = new Comparator<Ranked>() {
    @Override
    public int compare(Ranked lhs, Ranked rhs) {
      return CmpUtil.compare(lhs.rank, rhs.rank);
    }
  };
}
