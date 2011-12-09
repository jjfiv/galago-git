// BSD License (http://lemurproject.org/galago-license)


package org.lemurproject.galago.core.parse;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface DocumentStreamParser {
    public Document nextDocument() throws IOException;
}
