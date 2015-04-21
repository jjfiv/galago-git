package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * #bool()
 * Converts a boolean query into a "scoring" query so that Galago can output a set rather than a ranked list.
 * @author jfoley.
 */
public class BooleanScoreIterator extends TransformIterator implements ScoreIterator {
	public BooleanScoreIterator(NodeParameters np, IndicatorIterator iterator) {
		super(iterator);
	}

	@Override
	public double score(ScoringContext c) {
		return ((IndicatorIterator) iterator).indicator(c) ? 1.0 : 0.0;
	}

	@Override
	public boolean hasMatch(ScoringContext c) {
		IndicatorIterator iter = (IndicatorIterator) iterator;
		return iter.hasMatch(c) && iter.indicator(c);
	}

	@Override
	public double maximumScore() {
		return 1.0;
	}

	@Override
	public double minimumScore() {
		return 0.0;
	}

	@Override
	public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
		return iterator.getAnnotatedNode(sc);
	}
}
