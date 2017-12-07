package org.lemurproject.galago.tupleflow.slurm;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author jfoley
 */
public class SlurmModeStageExecutorTest {
  @Test
  public void javaToSlurmGBRequest() throws Exception {
    assertEquals(3, SlurmModeStageExecutor.javaToSlurmGBRequest("-Xmx1700m"));
    assertEquals(3, SlurmModeStageExecutor.javaToSlurmGBRequest("-Xmx2400m"));
    assertEquals(1, SlurmModeStageExecutor.javaToSlurmGBRequest("-Xmx512m"));
    assertEquals(2, SlurmModeStageExecutor.javaToSlurmGBRequest("-Xmx513m"));
    assertEquals(8, SlurmModeStageExecutor.javaToSlurmGBRequest("-Xmx7G"));
  }

}