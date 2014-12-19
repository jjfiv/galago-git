package org.lemurproject.galago.utility.debug;

/**
 * Created by jfoley on 12/19/14.
 */
public class NullCounter implements Counter {
  @Override
  public void increment() { }

  @Override
  public void incrementBy(int value) { }

  public static Counter instance = new NullCounter();
}
