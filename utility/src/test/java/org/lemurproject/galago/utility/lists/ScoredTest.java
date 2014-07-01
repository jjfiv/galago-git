package org.lemurproject.galago.utility.lists;

import org.junit.Test;
import org.lemurproject.galago.utility.CmpUtil;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ScoredTest {
  private static class ScoredTerm extends Scored {
    public final String term;

    public ScoredTerm(String term, double score) {
      super(score);
      this.term = term;
    }

    @Override
    public ScoredTerm clone(double score) {
      return new ScoredTerm(this.term, score);
    }

    @Override
    public String toString() {
      return String.format("WT<%s,%.3f>",term,score);
    }
  }

  @Test
  public void maxMinNormalizeTest() {
    List<ScoredTerm> terms = Arrays.asList(
      new ScoredTerm("foo", 3.0),
      new ScoredTerm("bar", 2.0),
      new ScoredTerm("baz", 1.0)
    );

    List<ScoredTerm> normalized = Scored.maxMinNormalize(terms);

    assertEquals(1.0, normalized.get(0).score, CmpUtil.epsilon);
    assertEquals(0.5, normalized.get(1).score, CmpUtil.epsilon);
    assertEquals(0.0, normalized.get(2).score, CmpUtil.epsilon);
  }

  @Test
  public void makeUniformTest() {
    List<ScoredTerm> terms = Arrays.asList(
      new ScoredTerm("foo", 3.0),
      new ScoredTerm("bar", 2.0),
      new ScoredTerm("baz", 1.0)
    );

    List<ScoredTerm> normalized = Scored.makeUniform(terms);

    assertEquals(1.0, normalized.get(0).score, CmpUtil.epsilon);
    assertEquals(1.0, normalized.get(1).score, CmpUtil.epsilon);
    assertEquals(1.0, normalized.get(2).score, CmpUtil.epsilon);
  }

  @Test
  public void relevanceModelWeightingTest() {
    // note log scores
    List<ScoredTerm> terms = Arrays.asList(
      new ScoredTerm("foo", -5.0),
      new ScoredTerm("bar", -6.0),
      new ScoredTerm("baz", -7.0)
    );

    List<ScoredTerm> normalized = Scored.rmNormalizeScores(terms);

    assertEquals(0.665, normalized.get(0).score, 0.01);
    assertEquals(0.245, normalized.get(1).score, 0.01);
    assertEquals(0.090, normalized.get(2).score, 0.01);
  }
}