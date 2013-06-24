// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.window;

/*
 * n-gram datatype
 *
 * @author sjh
 */
public class Window {
  
  // location in file for filtering (SpaceEfficient) //
  public int file;
  public long filePosition;

  // indexing data //
  public byte[] data;
  public long document;
  public int begin;
  public int end;

  public Window(int file, long filePosition, long document, int begin, int end, byte[] data){
    this.file = file;
    this.filePosition = filePosition;
    this.document = document;
    this.begin = begin;
    this.end = end;
    this.data = data;
  }
}
