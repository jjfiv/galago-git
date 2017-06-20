package org.lemurproject.galago.contrib.index.disk;

import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.utility.Parameters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * This class stores DocTermsInfo objects as a complete serialized byte array
 * by implementation of Serializable.  No fancy additions, skips, lengths, etc. needed.
 *
 * The object includes:
 *    PositionInfo class for annotation begin/end positions and offsets.
 *    TermInfo class for entity level information (term, frequencies, positions)
 *    DocTermsInfo class  containing terms listed for entire document.
 *
 * Since the annotations object structure is stable, this class is not an extension of 
 * some parent abstract class (such as with DocumentSerializer and WebDocumentSerializer.
 * This may need refactoring at some point in the future.
 *
 * @author smh
 */
public class ForwardIndexSerializer implements Serializable {


  public ForwardIndexSerializer (Parameters opts) {
    //super(opts);
  }


  //@Override
  /**
   * Comvert DocTermsInfo object to a compressed array of bytes
   * @throws IOException
   */
/*
  public static byte[] toBytes (DocTermsInfo docTermsInfo) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream ();
         ObjectOutputStream oos = new ObjectOutputStream (baos)) {
       oos.writeObject (docTermsInfo);
       oos.flush ();
       boas.flush ();
       oos.close ();
       boas.close ();
       return baos.toByteArray ();
    }
  }  //- end method toBytes
*/

  public static byte[] toBytes (DocTermsInfo docTermsInfo) throws IOException {

    try {
      //- First Serialize the DocTermsInfo object
      ByteArrayOutputStream baos = new ByteArrayOutputStream ();
      ObjectOutputStream oos = new ObjectOutputStream (baos);
      oos.writeObject (docTermsInfo);
      oos.flush ();
      byte[] serializedBytes = baos.toByteArray ();

      //- Then gzip compress the serialized object bytes      
      baos = new ByteArrayOutputStream (serializedBytes.length);
      GZIPOutputStream gzos = new GZIPOutputStream (baos);
      gzos.write (serializedBytes);
      gzos.finish ();
      gzos.close ();

      //- Convert the compressed serialized bytes to a Byte array
      baos.flush ();
      byte[] gzipedSerializedBytes = baos.toByteArray ();
      baos.close ();

      //- Return the compressed object serialation
      return gzipedSerializedBytes;
    }
    catch (Exception ex) {
      System.out.println ("Failure writing DocTermsInfo object to ObjectOutputStream." +
                          ex.toString());
      return null;
    }

  }  //- end method toBytes


  //Override
  /**
   * Convert a compressed byte array into a DocTermsInfo object
   * @throws IOException
   */
/*
  public static DocTermsInfo fromBytes(byte[] data) throws IOException {

    try (ByteArrayInputStream bais = new ByteArrayInputStream (data);
         ObjectInputStream ois = new ObjectInputStream (bais)) {
      bais.flush ();
      oos.flush ();
      bais.close ();
      oos.close ();

      return (DocTermsInfo)ois.readObject ();
    }
    catch (Exception ex) {
      System.out.println ("Failure reading DocTermsInfo object from ObjectInputStream." +
                          ex.toString());
      return null;
    }
  }
*/


  public static DocTermsInfo fromBytes(byte[] data) throws IOException {

    try {
      GZIPInputStream gzis = new GZIPInputStream (new ByteArrayInputStream (data));
      ObjectInputStream ois = new ObjectInputStream (gzis);
      DocTermsInfo docTermsInfo = (DocTermsInfo)ois.readObject ();
      ois.close ();
      gzis.close ();

      return docTermsInfo;
    }
    catch (Exception ex) {
      System.out.println ("Failure reading DocTermsInfo object from ObjectInputStream.\n" +
                          ex.toString());
      return null;
    }
  }  //- end method fromBytes

}  //- end class ForwardIndexSerializer
