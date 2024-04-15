import java.nio.ByteBuffer;

class Disk {
  public static int DISK_BYTE_SIZE = 500 * (int) Math.pow(2, 20);
  private int blockSizeInBytes;
  private ByteBuffer bytes;

  public Disk(int blockSizeInBytes) {
    this.blockSizeInBytes = blockSizeInBytes;
    this.bytes = ByteBuffer.allocate(DISK_BYTE_SIZE); // 500 MB;
  }

  public Block getBlock(int blockNumber) {
    byte[] blockBytes = new byte[blockSizeInBytes];
    bytes.position(getPositionFromBlockNumber(blockNumber)).get(blockBytes);
    return new Block(blockBytes);
  }

  public void writeBlock(int blockNumber, Block block) {
    bytes.position(getPositionFromBlockNumber(blockNumber));
    bytes.put(block.marshal());
  }

  private int getPositionFromBlockNumber(int blockNumber) {
    return (blockNumber-1) * blockSizeInBytes;
  }
}