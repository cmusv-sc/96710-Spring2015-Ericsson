package edu.cmu.sep;

import java.util.*;
import java.io.*;

public class Db {
    private String filename;
    public Db(String filename) {
        this.filename = filename;
    }

    public List<Integer> getData(int index) {
        Scanner s = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            String absoluteFilename = classLoader.getResource(this.filename).getFile();
            s = new Scanner(new File(absoluteFilename));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Integer> data = new ArrayList<Integer>();
        while (s.hasNext()) {
            String[] row = s.nextLine().split(",");
            int d = Integer.parseInt(row[index]);
            data.add(d);
        }
        return data;
    }
}
