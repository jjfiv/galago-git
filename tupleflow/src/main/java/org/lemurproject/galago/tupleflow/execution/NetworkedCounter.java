package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.Counter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class NetworkedCounter implements Counter {

  long count = 0;
  long lastFlushCount = Long.MIN_VALUE;
  String counterName;
  String stageName;
  String instance;
  String url;

  public NetworkedCounter(String counterName, String stageName, String instance, String url) {
    super();
    this.counterName = counterName;
    this.stageName = stageName;
    this.instance = instance;
    this.url = url;
  }

  public void increment() {
    incrementBy(1);
  }

  public void incrementBy(int value) {
    count += value;
  }

  public void flush() {
    // No need to send updates for counters that aren't changing.
    if (lastFlushCount == count) {
      return;
    }

    try {
      String fullUrl = String.format("%s/setcounter?counterName=%s&stageName=%s&instance=%s&value=%d",
              url, URLEncoder.encode(counterName, "UTF-8"),
              URLEncoder.encode(stageName, "UTF-8"),
              URLEncoder.encode(instance, "UTF-8"), count);
      connectUrl(fullUrl);
      lastFlushCount = count;
    } catch (Exception e) {
    }
  }

  public void connectUrl(String url) throws MalformedURLException, IOException {
    URLConnection connection = new URL(url).openConnection();

    // limit the connection attempt to 1 sec -- just in case.
    connection.setConnectTimeout(1000); // 1 s
    connection.connect();

    connection.getInputStream().close();
    connection.getOutputStream().close();
  }
}
