/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import junit.framework.TestCase;
import org.lemurproject.galago.contrib.learning.LearnQueryParameters;
import org.lemurproject.galago.core.tools.App;

/**
 *
 * @author sjh
 */
public class AppTest extends TestCase {
  
  public AppTest(String name){
    super(name);
  }
  
  public void testApp(){

    // check that App (from core package) can see the learner class (in contrib).
    assertTrue(App.appFunctions.containsKey("learner"));
    assertEquals(App.appFunctions.get("learner").getClass(), LearnQueryParameters.class);
    
  }
}
