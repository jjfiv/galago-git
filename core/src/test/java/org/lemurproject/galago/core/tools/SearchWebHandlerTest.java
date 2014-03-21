// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.tools;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertNotEquals;

/**
 *
 * @author trevor
 */
public class SearchWebHandlerTest {
    
  @Test
  public void testImage() throws IOException {
    SearchWebHandler handler = new SearchWebHandler(null);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    handler.retrieveImage(stream);
    assertNotEquals(0, stream.size());
  }
}
