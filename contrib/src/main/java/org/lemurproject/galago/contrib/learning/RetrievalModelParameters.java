/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.lemurproject.galago.tupleflow.Parameters;

import java.util.*;
import java.util.logging.Logger;

/**
 * Data type to store a set of query parameters for learning - each parameter
 * has a max and a min
 *
 * @author sjh
 */
public class RetrievalModelParameters {
  private static final Logger logger = Logger.getLogger(RetrievalModelParameters.class.getName());
  private TreeSet<String> names;
  private ParameterNormalizationRules normalizationRules;
  private Map<String, Parameters> parameterSpecifics;
  private TObjectDoubleHashMap<String> maxValues;
  private TObjectDoubleHashMap<String> minValues;
  private TObjectDoubleHashMap<String> ranges;

  public RetrievalModelParameters(List<Parameters> learnableParameters, List<Parameters> rules) {
    this.names = new TreeSet<String>();
    this.parameterSpecifics = new HashMap<String,Parameters>();
    this.maxValues = new TObjectDoubleHashMap<String>();
    this.minValues = new TObjectDoubleHashMap<String>();
    this.ranges = new TObjectDoubleHashMap<String>();

    for (Parameters param : learnableParameters) {
      String name = param.getString("name");
      this.parameterSpecifics.put(name, param);
      
      double max, min, range;
      if (param.isDouble("max")) {
        max = param.getDouble("max");
      } else {
        logger.info(String.format("Param %s max value not specified using 1.0", name));
        max = 1.0;
      }
      if (param.isDouble("min")) {
        min = param.getDouble("min");
      } else {
        logger.info(String.format("Param %s min value not specified using 0.0", name));
        min = 0.0;
      }
      range = max - min;
      // sanity check for impossible range
      assert range >= 0 : "Range of parameter " + name + " is zero or negative.";


      // now store values:
      names.add(name);
      maxValues.put(name, max);
      minValues.put(name, min);
      ranges.put(name, range);

      if (param.get("rangeLimiting", false)) {
        //  generate new rules based on this range:
        Parameters maxRule = new Parameters();
        maxRule.set("mode", "max");
        maxRule.set("value", max);
        maxRule.set("params", Collections.singletonList(name));

        Parameters minRule = new Parameters();
        minRule.set("mode", "min");
        minRule.set("value", min);
        minRule.set("params", Collections.singletonList(name));

        rules.add(maxRule);
        rules.add(minRule);
      }
    }

    this.normalizationRules = new ParameterNormalizationRules(rules);
  }

  public boolean includes(String param) {
    return this.names.contains(param);
  }

  public Set<String> getParams() {
    return this.names;
  }

  public ParameterNormalizationRules getRules() {
    return this.normalizationRules;
  }

  public double getRange(String param) {
    return this.ranges.get(param);
  }

  public double getMin(String param) {
    return this.minValues.get(param);
  }

  public double getMax(String param) {
    return this.maxValues.get(param);
  }

  public int getCount() {
    return this.names.size();
  }

  public Parameters getParameterSpecifics(String name) {
    return this.parameterSpecifics.get(name);
  }
}
