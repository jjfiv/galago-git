/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.hash;

import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;

import java.math.BigInteger;
import java.util.Random;

/**
 *
 * @author sjh
 */
public class UniversalStringHashFunction {

  public static UniversalStringHashFunction generate(long collectionLength, long universe, double errorCount, Random rnd) {

    double epsilon = errorCount / (double) collectionLength;
    BigInteger w = BigInteger.valueOf((long) Math.ceil((Math.E / epsilon)));

    // p1 must be larger than universe, and width
    BigInteger p1 = BigInteger.valueOf(universe).max(w).multiply(BigInteger.valueOf(3)).nextProbablePrime();
    // p2 must be larger than p1 (and universe, and width)
    BigInteger p2 = p1.multiply(BigInteger.valueOf(3)).nextProbablePrime();

    BigInteger a1 = BigInteger.ZERO;
    BigInteger a2 = BigInteger.ZERO;
    BigInteger b2 = BigInteger.ZERO;

    a1 = new BigInteger(p1.bitLength(), rnd);
    while (a1.compareTo(BigInteger.ZERO) <= 0 || a1.compareTo(p1) >= 0) {
      a1 = new BigInteger(p1.bitLength(), rnd);
    }

    a2 = new BigInteger(p2.bitLength(), rnd);
    while (a2.compareTo(BigInteger.ZERO) <= 0 || a2.compareTo(p2) >= 0) {
      a2 = new BigInteger(p2.bitLength(), rnd);
    }

    b2 = new BigInteger(p2.bitLength(), rnd);
    while (b2.compareTo(BigInteger.ZERO) <= 0 || b2.compareTo(p2) >= 0) {
      b2 = new BigInteger(p2.bitLength(), rnd);
    }

    // assert that 0 < a1 < p1
    assert a1.compareTo(BigInteger.ZERO) > 0;
    assert a1.compareTo(p1) < 0;
    // assert that 0 <= a2 < p2
    assert a2.compareTo(BigInteger.ZERO) > 0;
    assert a2.compareTo(p2) < 0;
    // assert that 0 <= b2 < p2
    assert b2.compareTo(BigInteger.ZERO) > 0;
    assert b2.compareTo(p2) < 0;
    // w should be less than p1
    assert w.compareTo(p1) < 0;
    // w should also be less than LONG
    assert w.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0;

    return new UniversalStringHashFunction(errorCount, p1, p2, a1, a2, b2, w);

  }

  public static UniversalStringHashFunction generate(double epsilon, long collectionLength, long universe, Random rnd) {

    double errorCount = epsilon / (double) collectionLength;
    BigInteger w = BigInteger.valueOf((long) Math.ceil((Math.E / epsilon)));

    // p1 must be larger than universe, and width
    BigInteger p1 = BigInteger.valueOf(universe).max(w).multiply(BigInteger.valueOf(3)).nextProbablePrime();
    // p2 must be larger than p1 (and universe, and width)
    BigInteger p2 = p1.multiply(BigInteger.valueOf(3)).nextProbablePrime();

    BigInteger a1 = BigInteger.ZERO;
    BigInteger a2 = BigInteger.ZERO;
    BigInteger b2 = BigInteger.ZERO;

    a1 = new BigInteger(p1.bitLength(), rnd);
    while (a1.compareTo(BigInteger.ZERO) <= 0 || a1.compareTo(p1) >= 0) {
      a1 = new BigInteger(p1.bitLength(), rnd);
    }

    a2 = new BigInteger(p2.bitLength(), rnd);
    while (a2.compareTo(BigInteger.ZERO) <= 0 || a2.compareTo(p2) >= 0) {
      a2 = new BigInteger(p2.bitLength(), rnd);
    }

    b2 = new BigInteger(p2.bitLength(), rnd);
    while (b2.compareTo(BigInteger.ZERO) <= 0 || b2.compareTo(p2) >= 0) {
      b2 = new BigInteger(p2.bitLength(), rnd);
    }

    // assert that 0 < a1 < p1
    assert a1.compareTo(BigInteger.ZERO) > 0;
    assert a1.compareTo(p1) < 0;
    // assert that 0 <= a2 < p2
    assert a2.compareTo(BigInteger.ZERO) > 0;
    assert a2.compareTo(p2) < 0;
    // assert that 0 <= b2 < p2
    assert b2.compareTo(BigInteger.ZERO) > 0;
    assert b2.compareTo(p2) < 0;
    // w should be less than p1
    assert w.compareTo(p1) < 0;
    // w should also be less than LONG
    assert w.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0;

    return new UniversalStringHashFunction(errorCount, p1, p2, a1, a2, b2, w);

  }
  private final double errorCount;
  private final BigInteger p1;
  private final BigInteger p2;
  private final BigInteger a1;
  private final BigInteger a2;
  private final BigInteger b2;
  private final BigInteger w;
  private final int bits;

  public UniversalStringHashFunction(double errorCount, BigInteger p1, BigInteger p2, BigInteger a1, BigInteger a2, BigInteger b2, BigInteger w) {
    this.errorCount = errorCount;
    this.p1 = p1;
    this.p2 = p2;
    this.a1 = a1;
    this.a2 = a2;
    this.b2 = b2;
    this.w = w;
    this.bits = (int) Math.ceil(Math.log(w.longValue()) / Math.log(2));
  }

  public UniversalStringHashFunction(Parameters p) {
    this.errorCount = p.getDouble("errorCount");
    this.p1 = new BigInteger(p.getString("p1"));
    this.p2 = new BigInteger(p.getString("p2"));
    this.a1 = new BigInteger(p.getString("a1"));
    this.a2 = new BigInteger(p.getString("a2"));
    this.b2 = new BigInteger(p.getString("b2"));
    this.w = new BigInteger(p.getString("w"));
    this.bits = (int) Math.ceil(Math.log(w.longValue()) / Math.log(2));
  }

  public Parameters toParameters() {
    Parameters p = Parameters.instance();
    p.set("errorCount", errorCount);
    p.set("p1", p1.toString());
    p.set("p2", p2.toString());
    p.set("a1", a1.toString());
    p.set("a2", a2.toString());
    p.set("b2", b2.toString());
    p.set("w", w.toString());
    p.set("bits", bits);
    return p;
  }

  public long hash(String data) {
    return hash(Utility.fromString(data));
  }

  public long hash(byte[] data) {
    BigInteger out = BigInteger.ZERO;
    BigInteger xi;
    for (int i = 0; i < data.length; i++) {
      // ensure each byte is positive
      int b = ((int) data[i]) + 128;
      xi = BigInteger.valueOf(b);
      out = out.add(a1.pow(i).multiply(xi));
    }
    out = out.mod(p1);
    out = out.multiply(a2).add(b2).mod(p2).mod(w);

    return out.longValue();
  }

  public long getWidth() {
    return w.longValue();
  }

  /**
   * testing and debugging *
   */
  public static void main(String[] argv) throws Exception {

    // number of items that will be hashed (no restrictions on content)
    long collectionLength = 1024;

    // static universe : array of bytes
    long universe = 256;

    // error rate
    double errorCount = 3.0;
    int rows = 3;
    int depth = 0;
    UniversalStringHashFunction[] hfs = new UniversalStringHashFunction[rows];

    for (int r = 0; r < rows; r++) {
      UniversalStringHashFunction hf = UniversalStringHashFunction.generate(collectionLength, universe, errorCount, new Random());
      depth = (int) hf.getWidth();
      hfs[r] = hf;

      System.err.println(hf.toParameters().toPrettyString());
    }

    long[][] data = new long[rows][depth];

    for (int item = 0; item < collectionLength; item++) {
      for (int r = 0; r < rows; r++) {
        int hash_value = (int) hfs[r].hash(Utility.fromString(Integer.toString(item)));
        data[r][hash_value] += 1;
      }
    }

    for (int item = 0; item < collectionLength; item++) {
      long est = collectionLength * 2;
      StringBuilder vals = new StringBuilder();
      for (int r = 0; r < rows; r++) {
        int hash_value = (int) hfs[r].hash(Utility.fromString(Integer.toString(item)));
        est = Math.min(est, data[r][hash_value]);
        vals.append(" ").append(data[r][hash_value]);
      }
      System.err.println(item + " " + est + " -- (" + vals.toString() + " )");
    }
  }
}
