/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Coordinate ascent learning algorithm.
 *
 * This class implements the linear ranking model known as Coordinate Ascent. It
 * was proposed in this paper: D. Metzler and W.B. Croft. Linear feature-based
 * models for information retrieval. Information Retrieval, 10(3): 257-274,
 * 2000.
 *
 *
 * issues - may want different step sizes -> possibly as a fraction of the range
 * of a variable (e.g. mu)
 *
 * @author bemike
 * @author sjh
 */
public class CoordinateAscentLearner extends Learner {
  // this is the max step size
  private static final double MAX_STEP = Math.pow(10, 6);

  // coord ascent specific parameters
  protected int maxIterations;
  protected HashMap<String, Double> minStepSizes;
  protected double minStepSize;
  protected double maxStepRatio;
  protected double stepScale;

  public CoordinateAscentLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    this.maxStepRatio = p.get("maxStepRatio", 0.5);
    this.stepScale = p.get("stepScale", 2.0);
    this.maxIterations = (int) p.get("maxIterations", 5);
    this.minStepSizes = new HashMap();
    this.minStepSize = p.get("minStepSize", 0.02);
    Parameters specialMinStepSizes = new Parameters();
    if (p.isMap("specialMinStepSize")) {
      specialMinStepSizes = p.getMap("specialMinStepSize");
    }
    for (String param : learnableParameters.getParams()) {
      minStepSizes.put(param, specialMinStepSizes.get(param, this.minStepSize));
    }
  }

  @Override
  public RetrievalModelInstance learn(RetrievalModelInstance parameterSettings) throws Exception {

    double best = this.evaluate(parameterSettings);
    logger.info(String.format("Initial parameter weights: %s Metric: %f. Starting optimization...", parameterSettings.toParameters().toString().toString(), best));

    boolean optimized = true;
    int iters = 0;
    while (optimized && iters < maxIterations) {
      List<String> optimizationOrder = new ArrayList(this.learnableParameters.getParams());
      Collections.shuffle(optimizationOrder, this.random);
      logger.info(String.format("Starting a new coordinate sweep...."));
      iters += 1;
      optimized = false;

      for (int c = 0; c < optimizationOrder.size(); c++) { // outer iteration
        String coord = optimizationOrder.get(c);
        logger.info(String.format("Iteration (%d of %d). Step (%d of %d). Starting to optimize coordinate (%s)...", iters, this.maxIterations, c + 1, optimizationOrder.size(), coord));
        double currParamValue = parameterSettings.get(coord); // Keep around the current parameter value
        // Take a step to the right 
        double step = this.minStepSizes.get(coord);
        if (parameterSettings.get(coord) != 0
                && step > (this.maxStepRatio * Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double rightBest = best;
        double rightStep = 0;
        boolean improving = true;
        // while we are ch
        while (improving) {
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr + step);
          double evaluation = this.evaluate(parameterSettings);
          logger.info(String.format("Coordinate (%s) ++%f... Metric: %f.", coord, step, evaluation));
          if (evaluation > rightBest || evaluation == best) {
            rightBest = evaluation;
            rightStep += step;
            step *= stepScale;
            // avoid REALLY BIG steps
            if(step > this.MAX_STEP){
              improving = false;
            }
          } else {
            improving = false;
          }
        }

        // revert changes
        parameterSettings.unsafeSet(coord, currParamValue);

        // Take a step to the right 
        step = this.minStepSizes.get(coord);
        if (parameterSettings.get(coord) != 0
                && step > (this.maxStepRatio * Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double leftBest = best;
        double leftStep = 0;
        improving = true;
        while (improving) {
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr - step);
          double evaluation = this.evaluate(parameterSettings);
          logger.info(String.format("Coordinate (%s) --%f... Metric: %f.", coord, step, evaluation));
          if (evaluation > leftBest || evaluation == best) {
            leftBest = evaluation;
            leftStep += step;
            step *= stepScale;
            // avoid REALLY BIG steps
            if(step > this.MAX_STEP){
              improving = false;
            }
          } else {
            improving = false;
          }
        }

        // revert changes
        parameterSettings.unsafeSet(coord, currParamValue);

        // pick a direction to move this parameter
        if ((rightBest > leftBest && rightBest > best) || rightBest > best) {
          optimized = true;
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr + rightStep);
          best = rightBest;
          logger.info(String.format("Finished optimizing coordinate (%s). ++%f. Metric: %f", coord, rightStep, best));

        } else if ((leftBest > rightBest && leftBest > best) || leftBest > best) {
          optimized = true;
          double curr = parameterSettings.get(coord);
          parameterSettings.unsafeSet(coord, curr - leftStep);
          best = leftBest;
          logger.info(String.format("Finished optimizing coordinate (%s). --%f. Metric: %f", coord, leftStep, best));

        } else {
          logger.info(String.format("Finished optimizing coordinate (%s). No Change. Best: %f", coord, best));
        }

        parameterSettings.normalize();
        logger.info(String.format("Current source weights: %s", parameterSettings.toString()));
      }
      logger.info(String.format("Finished coordinate sweep."));
    }

    logger.info(String.format("No changes in the current round or maximum number of iterations reached... Done optimizing."));
    logger.info(String.format("Best metric achieved: %s", best));
    return parameterSettings;
  }
}
