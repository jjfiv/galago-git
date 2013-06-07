/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Coordinate ascent learning algorithm.
 *
 * This class implements the linear ranking model known as Coordinate Ascent. It
 * was originally proposed in this paper: D. Metzler and W.B. Croft. Linear feature-based
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

  private double MAX_STEPS = 20.0; // in either direction.
  // coord ascent specific parameters
  protected int maxIterations;
  protected HashMap<String, Double> minStepSizes;
  protected double minStepSize;
  protected double maxStepRatio;
  protected double stepScale;
  protected boolean limitRange;

  public CoordinateAscentLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    this.maxStepRatio = p.get("maxStepRatio", 0.3);  // if a particular step is bigger than the value, reduce step to a third of the value.
    this.stepScale = p.get("stepScale", 2.0);
    this.maxIterations = (int) p.get("maxIterations", 5);
    this.minStepSizes = new HashMap();
    this.minStepSize = p.get("minStepSize", 0.02);
    this.limitRange = p.get("limitRange", false);

    this.MAX_STEPS = p.get("maxSteps", this.MAX_STEPS);

    Parameters specialMinStepSizes = new Parameters();
    if (p.isMap("specialMinStepSize")) {
      specialMinStepSizes = p.getMap("specialMinStepSize");
    }
    for (String param : learnableParameters.getParams()) {
      minStepSizes.put(param, specialMinStepSizes.get(param, this.minStepSize));
    }
  }

  @Override
  public RetrievalModelInstance learn() throws Exception {
    // loop for each random restart:
    final List<RetrievalModelInstance> learntParams = Collections.synchronizedList(new ArrayList());

    if (threading) {
      ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList());
      final CountDownLatch latch = new CountDownLatch(restarts);

      for (int i = 0; i < restarts; i++) {
        final RetrievalModelInstance settingsInstance;
        if (initialSettings.size() > i) {
          settingsInstance = initialSettings.get(i).clone();
        } else {
          settingsInstance = generateRandomInitalValues();
        }
        settingsInstance.setAnnotation("name", name + "-randomStart-" + i);


        Thread t = new Thread() {

          @Override
          public void run() {
            try {
              RetrievalModelInstance s = runCoordAscent(settingsInstance);
              s.setAnnotation("score", Double.toString(evaluate(s)));
              learntParams.add(s);
              synchronized (outputPrintStream) {
                outputPrintStream.println(s.toString());
                outputPrintStream.flush();
              }
            } catch (Exception e) {
              exceptions.add(e);
              synchronized (outputTraceStream) {
                outputTraceStream.println(e.toString());
                outputTraceStream.flush();
              }
            } finally {
              latch.countDown();
            }
          }
        };
        threadPool.execute(t);
      }

      while (latch.getCount() > 0) {
        logger.info(String.format("Waiting for %d threads.", latch.getCount()));
        try {
          latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // do nothing
        }
      }

      threadPool.shutdown();

      if (!exceptions.isEmpty()) {
        for (Exception e : exceptions) {
          System.err.println("Caught exception: \n" + e.toString());
          e.printStackTrace();
        }
      }

    } else {

      for (int i = 0; i < restarts; i++) {
        final RetrievalModelInstance settingsInstance;
        if (initialSettings.size() > i) {
          settingsInstance = initialSettings.get(i).clone();
        } else {
          settingsInstance = generateRandomInitalValues();
        }
        settingsInstance.setAnnotation("name", name + "-randomStart-" + i);
        try {
          RetrievalModelInstance s = runCoordAscent(settingsInstance);
          s.setAnnotation("score", Double.toString(evaluate(s)));
          learntParams.add(s);
          synchronized (outputPrintStream) {
            outputPrintStream.println(s.toString());
            outputPrintStream.flush();
          }
        } catch (Exception e) {
          System.err.println("Caught exception: \n" + e.toString());
          e.printStackTrace();
          synchronized (outputTraceStream) {
            outputTraceStream.println(e.toString());
            outputTraceStream.flush();
          }
        }
      }
    }

    // check if we have learnt some values
    if (learntParams.isEmpty()) {
      return generateRandomInitalValues();
    } else {
      RetrievalModelInstance best = learntParams.get(0);
      double bestScore = Double.parseDouble(best.getAnnotation("score"));

      for (RetrievalModelInstance inst : learntParams) {
        double score = Double.parseDouble(inst.getAnnotation("score"));
        if (bestScore < score) {
          best = inst;
          bestScore = score;
        }
      }

      best.setAnnotation("name", name + "-best");

      outputPrintStream.println(best.toString());
      outputPrintStream.flush();

      return best;
    }
  }

  public RetrievalModelInstance runCoordAscent(RetrievalModelInstance parameterSettings) throws Exception {

    double best = this.evaluate(parameterSettings);
    outputTraceStream.println(String.format("Initial parameter weights: %s Metric: %f. Starting optimization...", parameterSettings.toParameters().toString().toString(), best));

    boolean optimized = true;
    int iters = 0;
    while (optimized && iters < maxIterations) {
      List<String> optimizationOrder = new ArrayList(this.learnableParameters.getParams());
      Collections.shuffle(optimizationOrder, this.random);
      outputTraceStream.println(String.format("Starting a new coordinate sweep...."));
      iters += 1;
      optimized = false;

      for (int c = 0; c < optimizationOrder.size(); c++) { // outer iteration
        String coord = optimizationOrder.get(c);
        double upperLimit = parameterSettings.getMax(coord);
        double lowerLimit = parameterSettings.getMin(coord);

        outputTraceStream.println(String.format("Iteration (%d of %d). Step (%d of %d). Starting to optimize coordinate (%s)...", iters, this.maxIterations, c + 1, optimizationOrder.size(), coord));
        double currParamValue = parameterSettings.get(coord); // Keep around the current parameter value
        // Take a step to the right 
        double step = this.minStepSizes.get(coord);
        if (parameterSettings.get(coord) != 0
                && step > (Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double rightBest = best;
        double rightStep = 0;
        int stepCount = 0;
        boolean improving = true;
        boolean atLimit = false;

        while (improving) {
          parameterSettings.unsafeSet(coord, currParamValue + step);
          double evaluation = evaluate(parameterSettings);
          outputTraceStream.println(String.format("Coordinate (%s) (%f ++%f)... Metric: %f.", coord, currParamValue, step, evaluation));

          // while we are improving, or equal to the current best - 
          if (evaluation > rightBest || evaluation == best) {
            // record the step as the current 'best'
            rightBest = evaluation;
            rightStep = step;

            // scale the step size up
            step *= stepScale;

            // avoid REALLY BIG steps
            if (stepCount > this.MAX_STEPS) {
              improving = false;
            }
            stepCount += 1;

            // avoid exceeding upper bound
            if (limitRange && (currParamValue + step > upperLimit)) {
              if (atLimit || currParamValue == upperLimit) {
                improving = false;
              } else {
                atLimit = true;
                step = upperLimit - currParamValue;
                outputTraceStream.println("Hit limit : upper limit: " + step);
              }
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
                && step > (Math.abs(parameterSettings.get(coord)))) {
          // Reduce the step size for very small weights
          step = (this.maxStepRatio * Math.abs(parameterSettings.get(coord)));
        }
        double leftBest = best;
        double leftStep = 0;
        // reset
        stepCount = 0;
        improving = true;
        atLimit = false;

        while (improving) {
          parameterSettings.unsafeSet(coord, currParamValue - step);
          double evaluation = evaluate(parameterSettings);
          outputTraceStream.println(String.format("Coordinate (%s) (%f --%f)... Metric: %f.", coord, currParamValue, step, evaluation));
          if (evaluation > leftBest || evaluation == best) {
            leftBest = evaluation;
            leftStep = step;
            step *= stepScale;

            // avoid REALLY BIG steps
            if (stepCount > this.MAX_STEPS) {
              improving = false;
            }
            stepCount += 1;

            // avoid exceeding lower bound
            if (limitRange && (currParamValue - step < lowerLimit)) {
              if (atLimit || currParamValue == lowerLimit) {
                improving = false;
              } else {
                atLimit = true;
                step = currParamValue - lowerLimit;
                outputTraceStream.println("Hit lower limit : new step: " + step);
              }
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
          parameterSettings.unsafeSet(coord, currParamValue + rightStep);
          best = rightBest;
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). (%f ++%f). Metric: %f", coord, currParamValue, rightStep, best));
          outputTraceStream.flush();

        } else if ((leftBest > rightBest && leftBest > best) || leftBest > best) {
          optimized = true;
          parameterSettings.unsafeSet(coord, currParamValue - leftStep);
          best = leftBest;
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). (%f --%f). Metric: %f", coord, currParamValue, leftStep, best));
          outputTraceStream.flush();

        } else {
          outputTraceStream.println(String.format("Finished optimizing coordinate (%s). No Change (%f). Best: %f", coord, currParamValue, best));
          outputTraceStream.flush();
        }

        parameterSettings.normalize();
        outputTraceStream.println(String.format("Current source weights: %s", parameterSettings.toString()));
        outputTraceStream.flush();
      }
      outputTraceStream.println(String.format("Finished coordinate sweep."));
      outputTraceStream.flush();
    }

    outputTraceStream.println(String.format("No changes in the current round or maximum number of iterations reached... Done optimizing."));
    outputTraceStream.println(String.format("Best metric achieved: %s", best));
    outputTraceStream.flush();
    return parameterSettings;
  }
}
