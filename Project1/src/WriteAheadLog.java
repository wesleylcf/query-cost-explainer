import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Purpose of write ahead logs: https://www.postgresql.org/docs/current/wal-intro.html
 */
public class WriteAheadLog implements CrashRecovery {
    private String logFileName;
    private DataOutputStream logStream;
    public static Integer LOG_BYTE_SIZE = Record.RECORD_BYTE_SIZE + CrashRecovery.OPERATION.BYTE_SIZE;

    public WriteAheadLog(String logFileName) {
        this.logFileName = logFileName;
        openLogStream();
    }

    private void openLogStream() {
      try {
          logStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFileName, true)));
      } catch (IOException e) {
          e.printStackTrace();
          // Handle exception (e.g., throw or log)
      }
    }

  public void logOperation(CrashRecovery.OPERATION operation, Record r) {
    try {
      byte[] operationBytes = operation.toBytes();
      byte[] affectedRecordBytes = r.marshal();
      logStream.write(operationBytes);
      logStream.write(affectedRecordBytes);
      logStream.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  

  public void restore(StorageManager storageManager) {
    try {
      DataInputStream log = new DataInputStream(new FileInputStream(this.logFileName));
      while (log.available() >= LOG_BYTE_SIZE) {
        byte[] logEntryBytes = new byte[LOG_BYTE_SIZE];
        log.read(logEntryBytes);

        byte operationByte = logEntryBytes[0];
        byte[] affectedRecordBytes = Arrays.copyOfRange(logEntryBytes, OPERATION.BYTE_SIZE, LOG_BYTE_SIZE);

        CrashRecovery.OPERATION operation = CrashRecovery.OPERATION.fromByte(operationByte);
        Record affectedRecord = Record.unmarshal(affectedRecordBytes);

        applyOperation(storageManager, operation, affectedRecord);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      logStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void applyOperation(StorageManager storageManager, CrashRecovery.OPERATION operation, Record record) {
    switch (operation) {
      case INSERT:
        storageManager.insertRecord(record);
        break;
      case DELETE:
        storageManager.deleteRecord(record);
        break;
      default:
        System.out.println("Unsupported operation: " + operation);
    }
  }


  public void checkpoint() {
    // Close the current log stream temporarily
    closeLogStream();

    try {
      String checkpointLogFileName = logFileName + ".checkpoint";
      DataOutputStream checkpointLogStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(checkpointLogFileName)));

      DataInputStream originalLogStream = new DataInputStream(new FileInputStream(logFileName));
      byte[] buffer = new byte[LOG_BYTE_SIZE];
      while (originalLogStream.available() >= LOG_BYTE_SIZE) {
        originalLogStream.read(buffer);
        checkpointLogStream.write(buffer);
      }

      originalLogStream.close();
      checkpointLogStream.close();

      openLogStream();
    } catch (IOException e) {
      e.printStackTrace();
      // Handle the exception (e.g., throw or log)
    }
  }


  public void closeLogStream() {
    try {
      if (logStream != null) {
        logStream.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // Finalize method (called by the garbage collector before an object is reclaimed)
  @Override
  protected void finalize() throws Throwable {
    closeLogStream();
  }
}
