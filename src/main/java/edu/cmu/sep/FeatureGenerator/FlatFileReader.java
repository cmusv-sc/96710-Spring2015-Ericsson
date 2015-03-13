package edu.cmu.sep.FeatureGenerator;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Jacob on 3/9/15.
 */
public class FlatFileReader {
  private char buffer[] = new char[8192 + 1];
  private int bufferSize;
  private int index;

  private FileReader fr;
  private int maxFieldLength = 8192;
  private char d;
  char[] fieldBuf = new char[maxFieldLength];

  public FlatFileReader(String fileName, char d) throws IOException {
    this.d = d;
    fr = new FileReader(fileName);
    bufferSize = fr.read(buffer, 0, 8192);
    if (bufferSize == -1)
      return;
  }

  public String[] readRecord() {
    String[] record = new String [100];
    int recordNumber = 0;
    int fieldBufferPosition = 0;
    while (bufferSize > 0) {
      for (int i = index; i < bufferSize; i++) {
        char c = buffer[i];
        if (c != d && c != '\n' && c != '\r') {
          fieldBuf[fieldBufferPosition++] = c;
        } else {
          if (fieldBufferPosition > 0) {
            record[recordNumber++] = new String(fieldBuf, 0, fieldBufferPosition);
            fieldBufferPosition = 0;
          } else if (c == d) {
            record[recordNumber++] = new String(fieldBuf, 0, fieldBufferPosition);
          }
          if (c == '\n') {
            index = i + 1;
            return record;
          }
        }
      }
      bufferSize = refreshBuffer();
      index = 0;
    }
    if (fieldBufferPosition > 0) {
      record[recordNumber++] = new String(fieldBuf, 0, fieldBufferPosition);
      fieldBufferPosition = 0;
    }
    return record;
  }

  private int refreshBuffer() {
    try {
      return fr.read(buffer, 0, 8192);
    } catch (IOException e) {
      return -1;
    }
  }

  public void close() {
    try {
      fr.close();
    } catch (IOException e) {

    }
  }
}
