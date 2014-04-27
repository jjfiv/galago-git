/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.lemurproject.galago.tupleflow.Parameters;

import java.util.List;

/**
 *
 * @author sjh
 */
public class RetrievalModelInstance {

  RetrievalModelParameters params;
  TObjectDoubleHashMap<String> settings;
  Parameters outParams;

  private RetrievalModelInstance() {
  }

  public RetrievalModelInstance(RetrievalModelParameters params, Parameters settings) {
    this.params = params;
    this.settings = new TObjectDoubleHashMap<String>();
    this.outParams = new Parameters();

    for (String p : params.getParams()) {
      unsafeSet(p, settings.getDouble(p));
    }
//  System.err.println("Created: " + toString());
    normalize();
//  System.err.println("Normal: " + toString());

  }

  public double get(String p) {
    return settings.get(p);
  }

  public double getMax(String coord) {
    return params.getMax(coord);
  }

  public double getMin(String coord) {
    return params.getMin(coord);
  }
  
  /**
   * This function allows learning modules to violate the normalization rules
   */
  public void unsafeSet(String p, double v) {
    settings.put(p, v);
  }

  public void set(String p, double v) {
    settings.put(p, v);
    params.getRules().normalize(this);
  }

  public void normalize() {
    params.getRules().normalize(this);
  }

  @Override
  public RetrievalModelInstance clone() {
    RetrievalModelInstance lpi = new RetrievalModelInstance();
    lpi.params = this.params;
    lpi.settings = new TObjectDoubleHashMap<String>(this.settings);
    lpi.outParams = outParams.clone();
    return lpi;
  }

  public static RetrievalModelInstance average(List<RetrievalModelInstance> settings) {
    RetrievalModelInstance lpi = settings.get(0).clone();
    for (String param : lpi.params.getParams()) {
      double value = 0;
      for (RetrievalModelInstance setting : settings) {
        value += setting.get(param);
      }
      lpi.unsafeSet(param, value / settings.size());
    }
    // ensure that the normalization rules are satisfied.
    lpi.normalize();
    return lpi;
  }

  @Override
  public String toString() {
    return toParameters().toString();
  }

  public String toPrettyString() {
    return toParameters().toPrettyString();
  }

  public Parameters toParameters() {
    Parameters ps = outParams.clone();
    for (String p : params.getParams()) {
      ps.set(p, settings.get(p));
    }
    return ps;
  }

  public void setAnnotation(String key, String value) {
    outParams.set(key, value);
  }

  public String getAnnotation(String key) {
    return outParams.getString(key);
  }
}
