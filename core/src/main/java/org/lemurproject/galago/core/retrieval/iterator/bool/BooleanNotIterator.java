package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * This is going to work, but it's probably going to be slow:
 * NOT(sparse term) = A lot of documents to think about!
 * @author jfoley.
 */
public class BooleanNotIterator extends TransformIterator implements IndicatorIterator {
	private final IndicatorIterator iter;

	public BooleanNotIterator(NodeParameters np, IndicatorIterator iterator, LengthsIterator lengths) {
		// This is a terrible hack that uses the fact that the LengthsIterator will always have a hit for every document...
		super(lengths);
		this.iter = iterator;
	}

	@Override
	public boolean hasMatch(long document) {
		if(!iter.hasMatch(document)) {
			return true;
		}
		//TODO: return indicator(c);
		return false;
	}

	@Override
	public boolean indicator(ScoringContext c) {
		try {
			iter.syncTo(c.document);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if(iter.hasMatch(c.document)) {
			System.err.printf("HasMatch: %d, hit=%s", c.document, iter.indicator(c));
			return !iter.indicator(c);
		}
		return true;
	}

	@Override
	public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
		return null; // I've never used this feature, so I don't really know what to fill out.
	}
}
