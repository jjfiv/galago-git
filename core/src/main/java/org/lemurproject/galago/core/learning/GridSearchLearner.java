/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Very simple grid search learning algorithm - divides the n dimensional space
 * evenly using 'gridSize'. Searches from min to max for each dimension.
 *
 * Useful for very low values of n (e.g. 1,2,3) and low values of gridSize 
 * ( gridSize < 10 )
 *
 * @author sjh
 */
public class GridSearchLearner extends Learner {

  double gridSize;

  public GridSearchLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // collect local parameters
    gridSize = p.getDouble("gridSize");
  }

  @Override
  public Parameters learn(Parameters initialSettings) throws Exception {
    List<String> params = new ArrayList(this.learnableParameters);

    if (params.size() > 0) {
      // depth first grid search
      Parameters settings = new Parameters();
      gridLearn(params.remove(0), params, settings);
      return settings;
    } else {
      return new Parameters();
    }
  }

  private void gridLearn(String param, List<String> params, Parameters settings) throws Exception {

    double step = this.learnableParametersRange.get(param) / gridSize;
    double paramValue = this.learnableParametersMin.get(param);
    double bestScore = 0.0;
    double bestParamValue = paramValue;

    String nextParam = (params.size() > 0) ? params.remove(0) : null;

    while (paramValue < this.learnableParametersMax.get(param)) {
      settings.set(param, paramValue);

      // recurse to find the best settings of the other parameters given this setting
      if (nextParam != null) {
        gridLearn(nextParam, params, settings);
      }

      double score = this.evaluate(settings);
      if (score > bestScore) {
        bestScore = score;
        bestParamValue = paramValue;
      }
      // move to the next grid 
      paramValue += step;
    }

    // ensure when we return the settings contain the best found parameter value
    settings.set(param, bestParamValue);
  }
}
