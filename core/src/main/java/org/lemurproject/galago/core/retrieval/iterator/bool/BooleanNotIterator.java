package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * This is going to work, but it's probably going to be slow:
 * NOT(sparse term) = A lot of documents to think about!
 * @author jfoley.
 */
public class BooleanNotIterator extends NegativeTransformIterator<IndicatorIterator> implements IndicatorIterator {
	public BooleanNotIterator(NodeParameters np, IndicatorIterator iterator, LengthsIterator lengths) {
		// This is a terrible hack that uses the fact that the LengthsIterator will always have a hit for every document...
		super(iterator, lengths);
	}

	@Override
	public boolean hasMatch(ScoringContext context) {
		return indicator(context);
	}

	@Override
	public boolean indicator(ScoringContext c) {
		if(inner.hasMatch(c)) {
			return !inner.indicator(c);
		}
		return true;
	}

	@Override
	public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
		return null; // I've never used this feature, so I don't really know what to fill out.
	}
}
