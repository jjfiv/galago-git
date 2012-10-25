// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.tools;

import ciir.proteus.galago.thrift.*;
import com.google.gson.stream.JsonReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * @author irmarc
 */
public class TopicIndexBuilder extends AppFunction {

  @Override
  public String getName() {
    return "build-topics";
  }

  @Override
  public String getHelpString() {
    return "galago build-topics --input=<path> --output=<path>";
  }

  public class KVWriter implements Processor<KeyValuePair> {

    DiskBTreeWriter destination;

    public KVWriter(String path) throws Exception {
      destination = new DiskBTreeWriter(path);
    }

    public void process(KeyValuePair kvp) throws IOException {
      destination.add(new GenericElement(kvp.key, kvp.value));
    }

    public void close() throws IOException {
      destination.close();
    }
  }
  Sorter<KeyValuePair> sorter;
  HashMap<String, PrefixedTermMap> topicsToPages;
  HashMap<String, TermList> wordsToTopics;
  ByteArrayOutputStream byteStream = new ByteArrayOutputStream(32768);
  TTransport transport = new TIOStreamTransport(byteStream);
  TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();

  public void run(Parameters p, PrintStream output) throws Exception {

    if (p.isMap("pages")) {
      Parameters pagesP = p.getMap("pages");
      String inputPath = pagesP.getString("input");
      String outputPath = pagesP.getString("output");
      output.printf("Pages indexing: %s --> %s\n", inputPath, outputPath);
      sorter = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder(),
              null,
              new KVWriter(outputPath));
      topicsToPages = new HashMap<String, PrefixedTermMap>();
      JsonReader reader = new JsonReader(new FileReader(inputPath));
      reader.beginObject();
      while (reader.hasNext()) {
        readPageEntry(reader);
      }
      reader.endObject();
      for (String key : topicsToPages.keySet()) {
        writeEntry(key, topicsToPages.get(key));
      }
      // finish sort and flush.
      reader.close();
      sorter.close();
    }

    if (p.isMap("words")) {
      Parameters wordsP = p.getMap("words");
      String inputPath = wordsP.getString("input");
      String outputPath = wordsP.getString("output");
      output.printf("Words indexing: %s --> %s\n", inputPath, outputPath);
      sorter = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder(),
              null,
              new KVWriter(outputPath));
      wordsToTopics = new HashMap<String, TermList>();
      JsonReader reader = new JsonReader(new FileReader(inputPath));
      reader.beginObject();
      while (reader.hasNext()) {
        readWordEntry(reader);
      }
      reader.endObject();
      for (String key : wordsToTopics.keySet()) {
        writeEntry(key, wordsToTopics.get(key));
      }
      reader.close();
      sorter.close();
    }
  }

  public void readWordEntry(JsonReader reader) throws Exception {
    String topic = reader.nextName();
    TermList tl = new TermList();
    reader.beginObject();
    while (reader.hasNext()) {
      String word = reader.nextName();
      double score = reader.nextDouble();
      tl.addToTerms(new WeightedTerm(word, score));
      if (!wordsToTopics.containsKey(word)) {
        wordsToTopics.put(word, new TermList());
      }
      wordsToTopics.get(word).addToTerms(new WeightedTerm(topic, score));
    }
    writeEntry(topic, tl);
    reader.endObject();
  }

  public void readPageEntry(JsonReader reader) throws Exception {
    String pageName = reader.nextName();
    TermList tl = new TermList();
    String[] parts = pageName.split("###");
    String bookName = parts[0];
    String pageNum = parts[1];
    reader.beginObject();
    while (reader.hasNext()) {
      String topicName = reader.nextName();
      double score = reader.nextDouble();
      tl.addToTerms(new WeightedTerm(topicName.replace("topic", ""),
              score));
      // Insert into the reverse mapping
      if (!topicsToPages.containsKey(topicName)) {
        PrefixedTermMap p =
                new PrefixedTermMap(new HashMap<String, TermList>());
        topicsToPages.put(topicName, p);
      }
      Map<String, TermList> reverseMaps =
              topicsToPages.get(topicName).getTerm_lists();
      if (!reverseMaps.containsKey(bookName)) {
        reverseMaps.put(bookName, new TermList());
      }
      reverseMaps.get(bookName).addToTerms(new WeightedTerm(pageNum, score));
    }
    PrefixedTermMap ptMap = new PrefixedTermMap();
    ptMap.putToTerm_lists("topic", tl);
    writeEntry(pageName, ptMap);
    reader.endObject();
  }

  // Write to the sorter, which will put them in order and flush to disk.
  public void writeEntry(String key, TBase postingList) throws Exception {
    TProtocol protocol = protocolFactory.getProtocol(transport);
    postingList.write(protocol);
    KeyValuePair kvp = new KeyValuePair(Utility.fromString(key),
            byteStream.toByteArray());
    sorter.process(kvp);
    byteStream.reset();
  }
}