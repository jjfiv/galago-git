package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;

/**
 * Converts a boolean query into a "scoring" query so that Galago can output a set rather than a ranked list.
 * @author jfoley.
 */
public class BinaryScoreIterator extends TransformIterator implements ScoreIterator {
	public BinaryScoreIterator(IndicatorIterator iterator) {
		super(iterator);
	}

	@Override
	public double score(ScoringContext c) {
		return ((IndicatorIterator) iterator).indicator(c) ? 1.0 : 0.0;
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
