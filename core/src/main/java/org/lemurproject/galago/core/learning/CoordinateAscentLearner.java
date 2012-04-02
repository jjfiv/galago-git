/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class CoordinateAscentLearner extends Learner {
  
  // coord ascent specific parameters
  protected int maxIterations;
  protected double minStepSize;
  protected double maxStepRatio;
  protected double stepScale;
  
  public CoordinateAscentLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);
    
    this.minStepSize = p.get("minStepSize", 0.02);
    this.maxStepRatio = p.get("maxStepRatio", 0.5);
    this.stepScale = p.get("stepScale", 2.0);
    this.maxIterations = (int) p.get("maxIterations", 5);
  }

  @Override
  public Parameters learn(Parameters paramValues) throws Exception {

    double best = this.evaluate(paramValues);
    logger.info(String.format( "Initial parameter weights: %s Metric: %f. Starting optimization...", paramValues.toString(), best));
    
    boolean optimized = true;
    int iters = 0;
    while(optimized && iters < maxIterations){
      List<String> optimizationOrder = new ArrayList(this.learnableParameters);
      Collections.shuffle(optimizationOrder, this.random);
      logger.info(String.format("Starting a new coordinate sweep...."));
      iters += 1;
      optimized = false;

      for(int c=0; c<optimizationOrder.size(); c++){ // outer iteration
        String coord = optimizationOrder.get(c);
        logger.info(String.format("Iteration (%d of %d). Step (%d of %d). Starting to optimize coordinate (%s)...", iters, this.maxIterations, c+1, optimizationOrder.size(), coord));
        double currParamValue = paramValues.getDouble(coord); // Keep around the current parameter value
        // Take a step to the right 
        double step = this.minStepSize;
        if(paramValues.getDouble(coord) != 0 
                && step > (this.maxStepRatio * Math.abs( paramValues.getDouble(coord) ))  ){
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs( paramValues.getDouble(coord) ));
        }
        double rightBest = best;
        double rightStep = 0;
        boolean change = true;
        while(change){
          double curr = paramValues.getDouble(coord);
          paramValues.set(coord, curr + step);          
          double evaluation = this.evaluate(paramValues);
          logger.info(String.format("Coordinate (%s) ++%f... Metric: %f.", coord, step, evaluation));
          if(evaluation > rightBest){
            rightBest = evaluation;
            rightStep += step;
            step *= stepScale;
          } else {
            change = false;
          }
        }
        
        // revert changes
        paramValues.set(coord, currParamValue);

        // Take a step to the right 
        step = this.minStepSize;
        if(paramValues.getDouble(coord) != 0 
                && step > (this.maxStepRatio * Math.abs( paramValues.getDouble(coord) ))  ){
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs( paramValues.getDouble(coord) ));
        }
        double leftBest = best;
        double leftStep = 0;
        change = true;
        while(change){
          double curr = paramValues.getDouble(coord);
          paramValues.set(coord, curr - step);
          double evaluation = this.evaluate(paramValues);
          logger.info(String.format("Coordinate (%s) --%f... Metric: %f.", coord, step, evaluation));
          if(evaluation > leftBest){
            leftBest = evaluation;
            leftStep += step;
            step *= stepScale;
          } else {
            change = false;
          }
        }
        
        // revert changes
        paramValues.set(coord, currParamValue);

        // pick a direction to move this parameter
        if(rightBest > leftBest){
          optimized = true;
          double curr = paramValues.getDouble(coord);
          paramValues.set(coord, curr + rightStep);
          best = rightBest;
          logger.info(String.format("Finished optimizing coordinate (%s). ++%f. Metric: %f", coord, rightStep, best));

        } else if(leftBest > rightBest){
          optimized = true;
          double curr = paramValues.getDouble(coord);
          paramValues.set(coord, curr - leftStep);
          best = leftBest;
          logger.info(String.format("Finished optimizing coordinate (%s). --%f. Metric: %f", coord, leftStep, best));
        } else {
          logger.info(String.format("Finished optimizing coordinate (%s). No Change. Best: %f", coord, best));
        }
        
        paramValues = this.normalizeParameters(paramValues);
        logger.info(String.format("Current source weights: %s", paramValues.toString()));  
      }
      logger.info(String.format("Finished coordinate sweep."));
    }
    
    logger.info(String.format("No changes in the current round or maximum number of iterations reached... Done optimizing."));
    logger.info(String.format("Best metric achieved: %s", best));
    return paramValues;
  }
}
