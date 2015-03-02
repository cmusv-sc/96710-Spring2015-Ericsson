import java.util.*;
import java.io.*;

class Db {
    private String filename;
    public Db(String filename) {
        this.filename = filename;
    }

    public List<Integer> getData(int index) {
        Scanner s = null;
        try {
            s = new Scanner(new File(filename));
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
