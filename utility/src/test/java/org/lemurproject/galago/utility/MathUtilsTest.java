package org.lemurproject.galago.utility;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MathUtilsTest {

  @Test
  public void testBinomialCoeff() throws Exception {
    // choose(7, 3) = 35
    long result = MathUtils.binomialCoeff(7, 3);
    assertEquals(35, result);

    // Check when k > n-k
    // choose(5,3) = 10
    result = MathUtils.binomialCoeff(5, 3);
    assertEquals(result, 10);
  }

  @Test
  public void testLogSumExp() throws Exception {
    double val = MathUtils.logSumExp(new double[] { Math.log(2), Math.log(3) });
    assertEquals(Math.log(5), val, 0.1);
    assertEquals(Math.log(9), MathUtils.logSumExp(new double[] { Math.log(2), Math.log(3), Math.log(4) }), 0.1);
  }

  @Test
  public void testWeightedGeometricMean() {
    assertEquals(4.0, MathUtils.weightedGeometricMean(new double[] {2,1}, new double[] { 2, 4 }), 0.1);
  }

  @Test
  public void testLogWeightedGeometricMean() {
    assertEquals(Math.log(4), MathUtils.logWeightedGeometricMean(new double[] {2,1}, new double[] { Math.log(2), Math.log(4) }), 0.1);
  }


  @Test
  public void testWeightedLogSumExp() throws Exception {
    assertEquals(Math.log(11),
      MathUtils.weightedLogSumExp(
        new double[] {2, 1, 1},
        new double[] { Math.log(2), Math.log(3), Math.log(4) }),
      0.1);
  }

  @Test
  public void testClamp() {
    assertEquals(2, MathUtils.clamp(7, 0, 2), 0.001);
    assertEquals(0, MathUtils.clamp(-5, 0, 2), 0.001);
  }

  @Test
  public void testSigmoid() {
    assertTrue(MathUtils.sigmoid(100.0) <= 1.0);
    assertTrue(MathUtils.sigmoid(100.0) >= 0.0);
    assertEquals(0.5, MathUtils.sigmoid(0.0), 0.001);
    assertTrue(MathUtils.sigmoid(-100.0) >= 0.0);
    assertTrue(MathUtils.sigmoid(-100.0) <= 1.0);
  }
}