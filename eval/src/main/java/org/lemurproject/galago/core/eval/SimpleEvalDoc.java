package org.lemurproject.galago.core.eval;

import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.lists.Ranked;
import org.lemurproject.galago.utility.lists.Scored;

import java.util.Comparator;

/**
 * @author jfoley.
 */
public class SimpleEvalDoc extends Ranked implements EvalDoc {
  public String name;

  public SimpleEvalDoc(String name, int rank, double score) {
    super(rank, score);
    this.name = name;
  }

  public SimpleEvalDoc(EvalDoc evalDoc) {
    this(evalDoc.getName(), evalDoc.getRank(), evalDoc.getScore());
  }

  @Override
  public int getRank() {
    return rank;
  }

  @Override
  public double getScore() {
    return score;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Scored clone(double score) {
    return new SimpleEvalDoc(name, rank, score);
  }

  @Override
  public boolean equals(Object rhs) {
    if(rhs instanceof EvalDoc) {
      EvalDoc theOther = (EvalDoc) rhs;
      return this.rank == theOther.getRank() && this.score == theOther.getScore() && this.name.equals(theOther.getName());
    }
    return false;
  }


  public static Comparator<EvalDoc> byAscendingRank = new Comparator<EvalDoc>() {
    @Override
    public int compare(EvalDoc lhs, EvalDoc rhs) {
      return CmpUtil.compare(lhs.getRank(), rhs.getRank());
    }
  };

}
