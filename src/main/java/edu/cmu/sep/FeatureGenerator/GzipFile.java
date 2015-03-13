package edu.cmu.sep.FeatureGenerator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Created by Jacob on 3/9/15.
 */
public class GzipFile {
  private String input;
  private String output;

  public GzipFile(String file) {
    this.input = file;
    this.output = file.substring(0, file.length() - 3);
  }

  public String gunZipFile() {
    byte[] buffer = new byte[1024];

    try{
      GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(input));
      FileOutputStream out = new FileOutputStream(output);

      int len;
      while ((len = gzis.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }

      gzis.close();
      out.close();

      System.out.println("Unzip " + input +" Done.");

    }catch(IOException e){
      e.printStackTrace();
    }
    return output;
  }

}
