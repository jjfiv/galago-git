package org.lemurproject.galago.core.corpus;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.Parameters;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * @author jfoley.
 */
public abstract class DocumentSerializer {
  public Parameters opts;
  public DocumentSerializer(Parameters opts) {
    this.opts = opts;
  }

  /**
   * Get a new Tokenizer based on the parameter settings. Since Tokenizer is not assumed to be threadsafe,
   * we expect to allocate one for each request.
   * @return Tokenizer instance.
   */
  public Tokenizer getTokenizer() {
    return Tokenizer.create(opts);
  }
  /**
   * Convert a Document to an array of byte[] for serializing to a file.
   * @param input a galago Document object
   * @return a byte array representing the document
   * @throws IOException
   */
  public abstract byte[] toBytes(Document input) throws IOException;

  /**
   * Convert an input stream into a Document, this is the lowest level call, to be implemented by serialization methods.
   * @param stream inputStream to read the document.
   * @param components An object that describes what to load from the serialized document.
   * @return the Galago Document object.
   * @throws IOException
   */
  public abstract Document fromStream(DataInputStream stream, Document.DocumentComponents components) throws IOException;

  /**
   * Convert a galago DataStream into a Document
   * @throws IOException
   */
  public Document fromStream(DataStream stream, Document.DocumentComponents components) throws IOException {
    return fromStream(new DataInputStream(stream), components);
  }

  /**
   * Convert a byte array into a Document
   * @throws IOException
   */
  public Document fromBytes(byte[] data, Document.DocumentComponents selection) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    return fromStream(new DataInputStream(stream), selection);
  }

  /** @deprecated use create instead! */
  @Deprecated
  public static DocumentSerializer instance(Parameters opts) throws IOException {
    return create(opts);
  }

  /**
   * Construct a DocumentSerializer based upon the manifest or build parameters.
   * @param opts the manifest
   * @return a new DocumentSerializer
   * @throws IOException
   */
  public static DocumentSerializer create(Parameters opts) throws IOException {
    String serializerClass = opts.get("documentSerializerClass", WebDocumentSerializer.class.getName());

    try {
      Class<?> clazz = Class.forName(serializerClass);
      Constructor<?> constructor = clazz.getConstructor(Parameters.class);
      return (DocumentSerializer) constructor.newInstance(opts);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
