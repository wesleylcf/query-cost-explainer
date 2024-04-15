import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DataSeeder {
  public static void seed(String filePath, StorageManager storageManager) {
    ArrayList<Record> records = new ArrayList<Record>();

    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line;
      br.readLine(); // skip header row

      while ((line = br.readLine()) != null) {
          String[] parts = line.split("\t");

          if (parts.length == 3) {
              String tconst = parts[0];
              Float averageRating = Float.parseFloat(parts[1]);
              Integer numVotes = Integer.parseInt(parts[2]);

              Record record = new Record(tconst, averageRating, numVotes, (short) 0);
              records.add(record);
          } else {
              System.err.println(String.format("Expected 3 columns but received: %s", parts.toString()));
          }
      }

      int seedByteSize = records.size() * Record.RATING_BYTE_SIZE;
      if (records.size() * Record.RATING_BYTE_SIZE >= Disk.DISK_BYTE_SIZE) {
        throw new IllegalArgumentException(String.format("Seed data size(%d Bytes) exceeds disk size(%d Bytes)", seedByteSize, Disk.DISK_BYTE_SIZE));
      }
      for(Record r : records) {
        storageManager.insertRecord(r);
      }
      
      System.out.println("#####\tSuccessfully seeded data. Head:\t#####");
      for(Integer i = 0; i < Math.min(10, records.size()); i++) {
        System.out.println(records.get(i));
      }
      System.out.println();

    } catch (IOException e) {
        System.err.println(String.format("Could not read file at filePath %s", filePath));
        e.printStackTrace(); // Handle the exception according to your needs
    }
  }
}
