/*
 * BSD License (http://lemurproject.org/galago-license)

 */

package org.lemurproject.galago.core.retrieval.iterator;

import java.util.Set;

/**
 * Indicates that an iterator can have modifiers attached to it.
 * This would work really well as a Scala trait or a Ruby module,
 * but Java doesn't play that way.
 *
 * @author irmarc
 */
public interface ModifiableIterator {
    public void addModifier(String k, Object m);

    public Set<String> getAvailableModifiers();

    public boolean hasModifier(String key) ;

    public Object getModifier(String modKey);
}
