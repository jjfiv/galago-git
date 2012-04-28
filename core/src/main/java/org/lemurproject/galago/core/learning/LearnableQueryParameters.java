/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Data type to store a set of query parameters for learning - each parameter
 * has a max and a min
 *
 * @author sjh
 */
public class LearnableQueryParameters {

  private Logger logger;
  private List<String> names;
  private ParameterNormalizationRules normalizationRules;
  private TObjectDoubleHashMap<String> maxValues;
  private TObjectDoubleHashMap<String> minValues;
  private TObjectDoubleHashMap<String> ranges;

  public LearnableQueryParameters(List<Parameters> learnableParameters, List<Parameters> rules) {
    this.logger = Logger.getLogger(this.getClass().getName());
    this.names = new ArrayList();
    this.maxValues = new TObjectDoubleHashMap();
    this.minValues = new TObjectDoubleHashMap();
    this.ranges = new TObjectDoubleHashMap();

    for (Parameters param : learnableParameters) {
      String name = param.getString("name");
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
      assert range > 0 : "Range of parameter " + name + " is negative.";


      // now store values:
      names.add(name);
      maxValues.put(name, max);
      minValues.put(name, min);
      ranges.put(name, range);

      // also generate new rules based on this range:
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

    this.normalizationRules = new ParameterNormalizationRules(rules);
  }

  public List<String> getParams() {
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

  int getCount() {
    return this.names.size();
  }
}
