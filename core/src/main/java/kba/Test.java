/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package kba;

import java.io.FileInputStream;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class Test {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {

    FileInputStream fis = new FileInputStream(args[0]);
    TProtocol tp = new TBinaryProtocol.Factory()
        .getProtocol(new TIOStreamTransport(fis));
    StreamItem si = new StreamItem();

    while (fis.available() > 0) {
      try {

        si.read(tp);

        System.out.println(new String(si.getDoc_id()));

      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }

    /*
    // Deserializing thrift object
    TDeserializer deserializer = new TDeserializer(
        new TBinaryProtocol.Factory());

    RandomAccessFile f = new RandomAccessFile(args[0], "r");
    byte[] b = new byte[(int) f.length()];
    f.read(b);

    // deserializer.deserialize(thrift object, byte array);
    // When you deserialize the byte array is converted to the
    // thrift object that is passed as a parameter to this method
    deserializer.deserialize(si, b);
    // deserializer.partialDeserialize(si, b, fieldIdPathFirst, fieldIdPathRest)

    System.out.println(new String(si.getBody().getCleansed()));
    // System.out.println(new String(si.getTitle().getCleansed()));
    // System.out.println(new String(si.getAnchor().getCleansed()));
    */
  }
}
