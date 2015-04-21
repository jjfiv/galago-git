package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;

/**
 * @author jfoley.
 */
public abstract class NegativeTransformIterator<T extends BaseIterator> extends TransformIterator {

	protected final T inner;

	public NegativeTransformIterator(T iterator, LengthsIterator lengths) {
		// This is a terrible hack that uses the fact that the LengthsIterator will always have a hit for every document...
		super(lengths);
		this.inner = iterator;
	}

	@Override
	public abstract boolean hasMatch(ScoringContext context);
	@Override
	public void movePast(long document) throws IOException {
		iterator.movePast(document);
		inner.movePast(document);
	}
	@Override
	public void syncTo(long document) throws IOException {
		iterator.syncTo(document);
		inner.syncTo(document);
	}



}
