import java.nio.ByteBuffer;

public class Record {
  public static final Integer RECORD_HEADER_BYTE_SIZE = 2;
  public static final Integer UUID_BYTE_SIZE = 10;
  public static final Integer RATING_BYTE_SIZE = 4;
  public static final Integer VOTES_BYTE_SIZE = 4;
  public static final Integer RECORD_BYTE_SIZE = RECORD_HEADER_BYTE_SIZE + 2 + UUID_BYTE_SIZE + 2 + RATING_BYTE_SIZE + VOTES_BYTE_SIZE; // 24 Bytes


  private short isDeleted = 0;
  private byte[] padding1 = new byte[2];
  private String uuid;
  private byte[] padding2 = new byte[2];
  private Float averageRating;
  private Integer numVotes;

  public Record(String tconst, Float averageRating, Integer numVotes, short isDeleted) {
    this.uuid = tconst;
    this.averageRating = averageRating;
    this.numVotes = numVotes;
    this.isDeleted = isDeleted;
  }

  /**
   * Returns Byte[] of the current record
   */
  public byte[] marshal() {
    ByteBuffer buffer = ByteBuffer.allocate(RECORD_BYTE_SIZE); // Total size including padding bytes

    buffer.putShort(isDeleted);
    buffer.put(padding1);

    byte[] uuidBytes = uuid.getBytes();
    buffer.put(uuidBytes);
    buffer.position(4 + UUID_BYTE_SIZE); // uuid is variable sized but we want it fixed to UUID_BYTE_SIZE
    buffer.put(padding2);

    buffer.putFloat(averageRating);
    buffer.putInt(numVotes);
    return buffer.array();
  }

  /**
   * Converts a Byte[] back to a Record
   */
  public static Record unmarshal(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    short isDeleted = buffer.getShort();
    buffer.position(buffer.position() + 2);

    byte[] uuidBytes = new byte[UUID_BYTE_SIZE];
    buffer.get(uuidBytes);

    buffer.position(buffer.position() + 2);
    String uuid = new String(uuidBytes);

    float averageRating = buffer.getFloat();
    int numVotes = buffer.getInt();

    return new Record(uuid, averageRating, numVotes, isDeleted);
  }

  public boolean isTombstone() {
    return isDeleted == 1;
  }

  @Override
  public String toString() {
      return String.format("{ uuid:'%s', averageRating:%f, numVotes:%d }", this.uuid, this.averageRating, this.numVotes);
  }

  public String getUuid() {
    return uuid;
  }

  public Float getAverageRating() {
    return averageRating;
  }

  public Integer getNumVotes() {
    return numVotes;
  }
}