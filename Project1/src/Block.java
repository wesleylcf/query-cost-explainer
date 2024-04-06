import java.nio.ByteBuffer;

/**
 * Temporary Block interface object to interact with bytes in the disk
 */
public class Block {
  public static final Integer BLOCK_BYTE_SIZE = 200;
  public static final Integer BLOCK_HEADER_BYTE_SIZE = 4;
  public static final Integer MAX_RECORDS_PER_BLOCK = (BLOCK_BYTE_SIZE - BLOCK_HEADER_BYTE_SIZE)/Record.RECORD_BYTE_SIZE;
  private ByteBuffer bytes;

  public Block() {
    bytes = ByteBuffer.allocate(BLOCK_BYTE_SIZE);
    bytes.putInt(0); // block header contains number of records initially 0
  }

  public Block(byte[] bytes) {
    this.bytes =  ByteBuffer.wrap(bytes);
  }

  public void insertRecordAt(int position, Record record) {
    byte[] recordBytes = record.marshal();
    int numRecords = bytes.getInt(0);

    if (position < 0 || position > numRecords) {
        throw new IllegalArgumentException("Invalid position for record insertion");
    }

    bytes.position(BLOCK_HEADER_BYTE_SIZE + position * Record.RECORD_BYTE_SIZE);

    int remainingBytes = (numRecords - position) * Record.RECORD_BYTE_SIZE;
    byte[] remainingData = new byte[remainingBytes];
    bytes.get(remainingData);

    bytes.position(BLOCK_HEADER_BYTE_SIZE + position * Record.RECORD_BYTE_SIZE);

    bytes.put(recordBytes);
    bytes.put(remainingData);

    setRecordCount(numRecords + 1);
  }

  public void insertRecord(Record record) {
    byte[] recordBytes = record.marshal();
    int numRecords = bytes.getInt(0);

    if (recordBytes.length > BLOCK_BYTE_SIZE - BLOCK_HEADER_BYTE_SIZE - numRecords * Record.RECORD_BYTE_SIZE) {
        throw new IllegalArgumentException("Data size exceeds remaining block capacity");
    }

    // Move the position to the end of the block header
    bytes.position(BLOCK_HEADER_BYTE_SIZE + numRecords * Record.RECORD_BYTE_SIZE);

    bytes.put(recordBytes);
    setRecordCount(numRecords + 1);
  }

  public Record getRecordAt(int recordIndex) {
    int numRecords = bytes.getInt(0);

    if (recordIndex < 0 || recordIndex >= numRecords) {
        throw new IllegalArgumentException("Invalid record index");
    }

    int recordPosition = BLOCK_HEADER_BYTE_SIZE + recordIndex * Record.RECORD_BYTE_SIZE;

    return getRecordFromBytes(recordPosition);
  }

  public Record getRecordFromBytes(Integer bytePosition) {
    byte[] recordBytes = new byte[Record.RECORD_BYTE_SIZE];
    bytes.position(bytePosition).get(recordBytes);
    return Record.unmarshal(recordBytes);
  }

  public boolean isFull() {
    int numRecords = getRecordCount();
    return numRecords >= MAX_RECORDS_PER_BLOCK;
  }

  public Integer getRecordCount() {
    return bytes.getInt(0);
  }

  public void setRecordCount(Integer count) {
    bytes.putInt(0, count);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int recordCount = getRecordCount();
    for (int i = 0; i < recordCount; i++) {
      sb.append(String.format("%s\n", getRecordFromBytes(BLOCK_HEADER_BYTE_SIZE + i*Record.RECORD_BYTE_SIZE)));
    }
    return sb.toString();
  }

  public byte[] marshal() {
    return this.bytes.array();
  }
}
