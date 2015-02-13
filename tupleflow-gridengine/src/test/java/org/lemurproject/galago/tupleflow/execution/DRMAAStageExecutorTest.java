package org.lemurproject.galago.tupleflow.execution;

import org.junit.Test;

import static org.junit.Assert.*;

public class DRMAAStageExecutorTest {

  @Test
  public void smokeTest() throws Exception {
    try {
      assertNotNull(new DRMAAStageExecutor(new String[0]));
    } catch (UnsatisfiedLinkError e) {
      // This is the error you get when not running on GridEngine!
      assertEquals("no drmaa in java.library.path", e.getMessage());
    }
  }
}