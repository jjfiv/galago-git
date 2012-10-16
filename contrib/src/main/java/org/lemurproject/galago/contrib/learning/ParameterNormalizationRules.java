/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class ParameterNormalizationRules {

  public class Rule {
    public String mode; // sum, max, min
    public List<String> params;
    public double value;

    public Rule(String mode, List<String> params, double value) {
      this.mode = mode;
      this.params = params;
      this.value = value;
    }
  }
  private List<Rule> rules;

  public ParameterNormalizationRules(List<Parameters> rules){
    this.rules = new ArrayList();
    for (Parameters rule : rules) {
      this.rules.add(new Rule(rule.getString("mode"), (List<String>) rule.getList("params"), rule.getDouble("value")));
    }
  }
  
  /**
   * Applies a series rules to normalize parameter values - this function should
   * be applied before running queries - it will prevent values violating
   * important constraints.
   */
  public void normalize(RetrievalModelInstance settings) {
    for (Rule rule : rules) {
      if (rule.mode.startsWith("sum")) { // rule: sums to value //
        double total = 0.0;
        for (String p : rule.params) {
          total += settings.get(p);
        }
        double normalizer = rule.value / total;
        for (String p : rule.params) {
          settings.unsafeSet(p, settings.get(p) * normalizer);
        }

      } else if (rule.mode.startsWith("max")) {
        for (String p : rule.params) {
          if (settings.get(p) > rule.value) {
            settings.unsafeSet(p, rule.value);
          }
        }
      } else if (rule.mode.startsWith("min")) {
        for (String p : rule.params) {
          if (settings.get(p) < rule.value) {
            settings.unsafeSet(p, rule.value);
          }
        }
      } else {
        throw new RuntimeException("Don't know how to apply: " + rule);
      }
    }
  }
}
