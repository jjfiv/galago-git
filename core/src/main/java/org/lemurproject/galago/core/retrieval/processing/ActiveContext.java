/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 *
 * @author irmarc
 */
public interface ActiveContext {

    public void checkIterator(Node node, MovableIterator iterator);
}
