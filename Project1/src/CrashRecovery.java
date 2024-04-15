public interface CrashRecovery {

  /**
   * Only INSERT and DELETE required for recovery since UPDATE is essentially DELETE + INSERT
   */
  enum OPERATION {
    INSERT((byte) 0),
    DELETE((byte) 1);

    private final byte value;
    public static Integer BYTE_SIZE = 1;

    OPERATION(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public byte[] toBytes() {
        return new byte[]{value};
    }

    public static OPERATION fromByte(byte b) {
      if (b == (byte) 0) {
        return INSERT;
      } else if (b == (byte) 1) {
        return DELETE;
      } else {
        throw new IllegalArgumentException("bytes did not correspond to an enum value");
      }
    }
  }

  // Log an operation
  void logOperation(OPERATION operation, Record r);

  // Restore DB from Log after a crash
  void restore(StorageManager storageManager);

  // Periodically garbage collect logs to keep the size of the crash recovery component small
  void checkpoint();
}
