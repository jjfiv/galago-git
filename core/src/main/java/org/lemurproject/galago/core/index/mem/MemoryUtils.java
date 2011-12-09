// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;


public class MemoryUtils {

  public static int[] ensureCapacity(int[] array, int size){
    if(array.length > size){
      return array;
    } else {
      int[] new_array;
      int new_array_size = array.length;
      while( new_array_size <= size ){
        new_array_size *= 2;
      }
      new_array = new int[new_array_size];
      System.arraycopy(array, 0, new_array, 0, array.length);
      return new_array;
    }
  }

  public static String[] ensureCapacity(String[] array, int size){
    if(array.length > size){
      return array;
    } else {
      String[] new_array;
      int new_array_size = array.length;
      while( new_array_size <= size ){
        new_array_size *= 2;
      }
      new_array = new String[new_array_size];
      System.arraycopy(array, 0, new_array, 0, array.length);
      return new_array;
    }
  }


}
