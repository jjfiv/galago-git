// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.lists;

import gnu.trove.list.array.TDoubleArrayList;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.MathUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author jfoley
 */
public abstract class Scored implements Serializable {
  public double score;

  public Scored(double score) {
    this.score = score;
  }

  public abstract Scored clone(double score);


  @SuppressWarnings("unchecked")
  public static <T extends Scored> List<T> rmNormalizeScores(List<T> terms) {
    if(terms.isEmpty()) return terms;

    TDoubleArrayList values = new TDoubleArrayList();
    for (T wt : terms) {
      values.add(wt.score);
    }
    double logSumExp = MathUtils.logSumExp(values.toArray());

    ArrayList<T> newTerms = new ArrayList<T>(terms.size());
    for(T wt : terms) {
      double posterior = wt.score - logSumExp;
      newTerms.add((T) wt.clone(Math.exp(posterior)));
    }

    return newTerms;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Scored> List<T> makeUniform(List<T> terms) {
    if(terms.isEmpty()) return terms;

    ArrayList<T> result = new ArrayList<T>(terms.size());
    for(T wt : terms) {
      result.add((T) wt.clone(1.0));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Scored> List<T> maxMinNormalize(List<T> terms) {
    if(terms.isEmpty()) return terms;

    ArrayList<T> result = new ArrayList<T>(terms.size());
    double max = Double.NEGATIVE_INFINITY;
    double min = Double.POSITIVE_INFINITY;
    for(T wt : terms) {
      if(wt.score < min) min = wt.score;
      if(wt.score > max) max = wt.score;
    }
    for(T wt : terms) {
      double newScore = (wt.score - min) / (max - min);
      result.add((T) wt.clone(newScore));
    }

    return result;
  }

  public static <T extends Scored> List<T> normalize(String method, List<T> input) {
    List<T> results;
    if("rm".equals(method)) {
      results = rmNormalizeScores(input);
    } else if("uniform".equals(method)) {
      results = makeUniform(input);
    } else if("maxmin".equals(method)) {
      results = maxMinNormalize(input);
    } else if("none".equals(method)) {
      results = input;
    } else {
      throw new IllegalArgumentException("Unknown normalization scheme: "+method);
    }
    return results;
  }

  public static Comparator<? super Scored> byScore = new Comparator<Scored>() {
    @Override
    public int compare(Scored lhs, Scored rhs) {
      return CmpUtil.compare(lhs.score, rhs.score);
    }
  };
}
