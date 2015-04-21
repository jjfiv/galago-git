/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author sjh
 */
public abstract class ExtentConjunctionIterator extends ConjunctionIterator implements DataIterator<ExtentArray>, ExtentIterator, CountIterator {

    protected ExtentArray extentCache;
    protected byte[] key;
    protected ScoringContext cachedContext = null;

    public ExtentConjunctionIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
        super(parameters, iterators);
        this.extentCache = new ExtentArray();
    }

    @Override
    public String getValueString(ScoringContext c) throws IOException {
        ArrayList<String> strs = new ArrayList<String>();
        ExtentArrayIterator eai = new ExtentArrayIterator(extents(c));
        while (!eai.isDone()) {
            strs.add(String.format("[%d, %d]", eai.currentBegin(), eai.currentEnd()));
            eai.next();
        }
        return Utility.join(strs, ",");
    }

    @Override
    public ExtentArray extents(ScoringContext c) {
        this.loadExtents(c);
        return extentCache;
    }

    @Override
    public ExtentArray data(ScoringContext c) {
        return extents(c);
    }

    @Override
    public int count(ScoringContext c) {
        return extents(c).size();
    }

    public void loadExtents(long document) {
        ScoringContext c = new ScoringContext();
        c.document = document;
        c.cachable = false; // TODO: not sure if this should be true or false

        // reset the extentCache
        extentCache.reset();
        extentCache.setDocument(c.document);

        // if we're done - quit now 
        //  -- (leaving extentCache object empty just in cast someone asks for them.)
        if (isDone()) {
            return;
        }

        loadExtentsCommon(c);
    }

    public void loadExtents(ScoringContext c) {

        if (c.equals(cachedContext)) {
            assert (this.extentCache.getDocument() == c.document);
            return; // we already have it computed
        }
        // set current context as cached
        if (cachedContext == null || (cachedContext.getClass() != c.getClass())) {
            cachedContext = c.getPrototype();
        } else {
            cachedContext.setFrom(c);
        }

        // reset the extentCache
        extentCache.reset();
        extentCache.setDocument(c.document);

        // if we're done - quit now 
        //  -- (leaving extentCache object empty just in cast someone asks for them.)
        if (isDone()) {
            return;
        }

        loadExtentsCommon(c);

    }

    public void loadExtentsCommon(ScoringContext c) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
        // ensure extentCache are loaded
        this.loadExtents(c);

        String type = "extent";
        String className = this.getClass().getSimpleName();
        String parameters = "";
        long document = currentCandidate();
        boolean atCandidate = hasMatch(c);
        String returnValue = extents(c).toString();
        List<AnnotatedNode> children = new ArrayList<AnnotatedNode>();
        for (BaseIterator child : this.iterators) {
            children.add(child.getAnnotatedNode(c));
        }
        return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public boolean hasMatch(ScoringContext context) {

        if (super.hasMatch(context)) {

            loadExtents(context);

            return (extentCache.getDocument() == context.document && extentCache.size() > 0);
        }
        return false;
    }

    @Override
    public boolean indicator(ScoringContext c) {
        return count(c) > 0;
    }
}
