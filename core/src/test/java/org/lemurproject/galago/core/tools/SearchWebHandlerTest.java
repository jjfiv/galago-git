// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.core.tools.SearchWebHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class SearchWebHandlerTest extends TestCase {
    
    public SearchWebHandlerTest(String testName) {
        super(testName);
    }

    public void testImage() throws IOException {
        SearchWebHandler handler = new SearchWebHandler(null);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        handler.retrieveImage(stream);
    }
}
