/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.tupleflow;

/**
 * If this list is tobe extended - append to the end of the list. - this ensures
 * that all previously written files are compatible
 *
 * @author sjh
 */
public enum CompressionType {

  UNSPECIFIED, NONE, VBYTE, GZIP;

  public static byte toByte(CompressionType c) {
    switch (c) {
      case UNSPECIFIED:
        return (byte) 0;
      case NONE:
        return (byte) 1;
      case VBYTE:
        return (byte) 2;
      case GZIP:
        return (byte) 3;
      default:
        return (byte) 0;
    }
  }

  public static CompressionType fromByte(byte c) {
    switch (c) {
      case 0:
        return UNSPECIFIED;
      case 1:
        return NONE;
      case 2:
        return VBYTE;
      case 3:
        return GZIP;
      default:
        return UNSPECIFIED;
    }
  }
}
