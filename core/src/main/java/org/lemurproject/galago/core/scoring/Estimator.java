/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.scoring;

import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.processing.SoftDeltaScoringContext;

/**
 * An interface to indicate that the exact score is unknown. The implementing
 * class can give highs and lows for score.
 *
 * @author irmarc
 */
public interface Estimator {

  public double[] estimate(SoftDeltaScoringContext context);

  public void adjustEstimate(SoftDeltaScoringContext context, EstimatedDocument document, int idx);

  public double[] estimateWithUpdate(SoftDeltaScoringContext context, int idx);
}
