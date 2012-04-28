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
 * Useful for very low values of n (e.g. 1,2,3) and low values of gridSize (
 * gridSize < 10 )
 *
 *
 *

 *
 * @author sjh
 */
public class GridSearchLearner extends Learner {
  
  double gridSize;
  
  public GridSearchLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // check required parameters
    assert (p.isLong("gridSize")) : this.getClass().getName() + " requires `gridSize' parameter, of type long";

    // collect local parameters
    gridSize = p.getLong("gridSize");
  }
  
  @Override
  public LearnableParameterInstance learn(LearnableParameterInstance initialSettings) throws Exception {
    List<String> params = new ArrayList(initialSettings.params.getParams());
    
    if (params.size() > 0) {
      // depth first grid search
      LearnableParameterInstance best = gridLearn(0, params, initialSettings.clone());
      
      best.normalize();
      logger.info("Best settings: " + best.toParameters().toString());
      return best;
    }
    
    logger.info("No learnable parameters found. Quitting.");
    return initialSettings;
  }
  
  private LearnableParameterInstance gridLearn(int i, List<String> params, LearnableParameterInstance settings) throws Exception {
    
    String param = params.get(i);
    
    double paramValue = this.learnableParameters.getMin(param);
    double maxParamValue = this.learnableParameters.getMax(param);
    double step = this.learnableParameters.getRange(param) / gridSize;
    // ensure a minimum step size
    if(step == 0){
      step = 0.01;
    }
    
    double bestScore = 0.0;
    double bestParamValue = paramValue;
    
    while (paramValue <= maxParamValue) {
      settings.unsafeSet(param, paramValue);

      // recurse to find the best settings of the other parameters given this setting
      if (i + 1 < params.size()) {
        gridLearn(i + 1, params, settings);
      }
      
      double score = this.evaluate(settings);
      
      if (score > bestScore) {
        bestScore = score;
        bestParamValue = paramValue;
        logger.info(String.format("Found better param value: %s = %f ; score = %f\nSettings: %s", param, paramValue, score, settings));
      }
      // move to the next grid line
      paramValue += step;
    }

    // ensure when we return the settings contain the best found parameter value
    settings.unsafeSet(param, bestParamValue);
    
    
    
    return settings;
  }
}
