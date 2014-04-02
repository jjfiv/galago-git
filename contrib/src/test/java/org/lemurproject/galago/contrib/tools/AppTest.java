/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.contrib.Main;
import org.lemurproject.galago.contrib.learning.LearnQueryParameters;
import org.lemurproject.galago.core.tools.App;

/**
 *
 * @author sjh
 */
public class AppTest {
  @Test
  public void testApp() throws Exception {
    Main.main(new String[0]);

    // check that App (from core package) can see the learner class (in contrib).
    Assert.assertTrue(App.appFunctions.containsKey("learner"));
    Assert.assertEquals(App.appFunctions.get("learner").getClass(), LearnQueryParameters.class);
    
  }
}
