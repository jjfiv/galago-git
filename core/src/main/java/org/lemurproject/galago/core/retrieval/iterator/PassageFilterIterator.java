// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author irmarc
 */
public class PassageFilterIterator extends TransformIterator implements ExtentIterator, CountIterator {

    ExtentIterator extentIterator;
    int begin, end;
    long docid;
    ExtentArray cached;
    protected byte[] key;

    public PassageFilterIterator(NodeParameters parameters, ExtentIterator extentIterator) {
        super(extentIterator);
        this.extentIterator = extentIterator;
        this.cached = new ExtentArray();
        docid = -1;
    }

    /**
     * Filters out extents that are not in the range of the current passage
     * window.
     *
     * @return
     */
    @Override
    public ExtentArray extents(ScoringContext c) {
        PassageScoringContext passageContext = ((c instanceof PassageScoringContext) ? (PassageScoringContext) c : null);

        if (passageContext == null) {
            return extentIterator.extents(c);
        }

        if (docid != passageContext.document || begin != passageContext.begin
                || end != passageContext.end) {
            loadExtents(c);
        }
        return cached;
    }

    private void loadExtents(ScoringContext c) {
        PassageScoringContext passageContext = ((c instanceof PassageScoringContext) ? (PassageScoringContext) c : null);
        if (passageContext == null) {
            throw new IllegalArgumentException("Expected PassageScoringContext, got a ScoringContext");
        }

        cached.reset();
        ExtentArray internal = extentIterator.extents(c);

        if (passageContext != null) {
            for (int i = 0; i < internal.size(); i++) {
                if (internal.begin(i) >= passageContext.begin
                        && internal.end(i) <= passageContext.end) {
                    cached.add(internal.begin(i), internal.end(i));
                }
            }
            docid = passageContext.document;
            begin = passageContext.begin;
            end = passageContext.end;
        }
    }

    @Override
    public ExtentArray data(ScoringContext c) {
        return extents(c);
    }

    @Override
    public int count(ScoringContext c) {
        return extents(c).size();
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
        String type = "extent";
        String className = this.getClass().getSimpleName();
        String parameters = "";
        long document = currentCandidate();
        boolean atCandidate = hasMatch(c.document);
        String returnValue = extents(c).toString();
        List<AnnotatedNode> children = Collections.singletonList(extentIterator.getAnnotatedNode(c));

        return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
}
