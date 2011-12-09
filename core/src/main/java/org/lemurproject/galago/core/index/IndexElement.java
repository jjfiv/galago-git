// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.OutputStream;

/**
 *
 * @author trevor
 */
public interface IndexElement {
    public byte[] key();
    public long dataLength();
    public void write(final OutputStream stream) throws java.io.IOException;
}
