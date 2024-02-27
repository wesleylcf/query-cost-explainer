/**
 * Blocks are logical units representing a particular slice of the Disk
 * This class is to encapsulate the logic for interactions between Disk and Block
 */
class StorageManager {
  private Integer numRecords = 0;
  private int occupiedBlocks = 0;
  Disk disk;
  StorageConfiguration config;
  CrashRecovery recovery;

  public StorageManager(Disk disk, StorageConfiguration storageConfiguration, CrashRecovery recovery) {
    this.disk = disk;
    this.config = storageConfiguration;
    this.recovery = recovery;
  }

  /**
   * We do an append only log which is performant as it makes use of sequential writes.
   * @param record Record to be inserted
   */
  public void insertRecord(Record r) {
    recovery.logOperation(CrashRecovery.OPERATION.INSERT, r);
    int recordPositionInDisk; 
    if (occupiedBlocks == 0) {
      Block block = new Block();
      occupiedBlocks ++;
      block.insertRecord(r);
      disk.writeBlock(1, block);
      recordPositionInDisk = Record.RECORD_BYTE_SIZE;
    } else {
      Block block = disk.getBlock(occupiedBlocks);
      if (block.isFull()) {
        block = new Block();
        occupiedBlocks ++;
      }
      block.insertRecord(r);
      disk.writeBlock(occupiedBlocks, block);
      recordPositionInDisk = occupiedBlocks * Block.BLOCK_BYTE_SIZE + (block.getRecordCount()-1) * Record.RECORD_BYTE_SIZE;
    }
    numRecords ++;
    this.checkAndRunCompaction();
    // Update b tree with recordPositionInDisk
  }

  /**
   * Instead of deleting on shift, mark the record with RecordTombstone and batch delete and shift when tombstone exceed 20%
   * @param record Record to delete
   */
  public void deleteRecord(Record r) {
    recovery.logOperation(CrashRecovery.OPERATION.DELETE, r);
    Record recordTombstone = new Record(r.getUuid(), r.getAverageRating(), r.getNumVotes(), (short) 1);
    insertRecord(recordTombstone);
    this.checkAndRunCompaction();
  }

  public void readRecordByPrimaryKey() {
    // Get (block #, record #) from default B tree
    // If valid block # and record # do constant time lookup. Otherwise do linear lookup.
  }

  public void updateRecordByPrimaryKey() {
    // Call readRecordByPrimaryKey
    // Call deleteRecord
    // Call insertRecord with the updated properties
  }

  /**
   * For simplicity assume all blocks are full
   */
  public float getDiskUtilization() {
    return (float) (occupiedBlocks * Block.BLOCK_BYTE_SIZE) / Disk.DISK_BYTE_SIZE;
  }

  /**
   * Check if disk utilization is over 90%.
   * If so delete tombstones and reclaim space to reduce fragmentation
   */
  private void checkAndRunCompaction() {
    if (getDiskUtilization() < this.config.getCompactionThreshold()) {
      return;
    }
    int currentBlockNumber = 1;
    int currentIndexInBlock = 0;  // Index in the current block

    // Iterate through blocks and process tombstones
    for (int blockNumber = 1; blockNumber <= occupiedBlocks; blockNumber++) {
        Block currentBlock = disk.getBlock(blockNumber);
        int numRecords = currentBlock.getRecordCount();

        // Iterate through records in the current block
        for (int j = 0; j < numRecords; j++) {
            Record record = currentBlock.getRecordAt(j);
            if (record.isTombstone()) {
              numRecords --;
              continue;
            }
            // Move valid records to the current block and index in block
            Block block = disk.getBlock(currentBlockNumber);
            block.insertRecordAt(currentIndexInBlock, record);
            currentIndexInBlock++;

            if (block.isFull()) {
                currentBlockNumber++;
                currentIndexInBlock = 0;
            }
        }
    }
  }
  public void printState(Boolean verbose) {
    System.out.println("#####\tPrinting state of Storage\t#####");
    if(verbose) {
      for (int blockNumber = 1; blockNumber <= occupiedBlocks; blockNumber ++) {
        System.out.println(disk.getBlock(blockNumber));
      }
    }
    System.out.println(String.format("Number of records: %d", numRecords));
    System.out.println(String.format("Size of record in bytes: %d", Record.RECORD_BYTE_SIZE));
    System.out.println(String.format("Max number of records in a block: %d", Block.MAX_RECORDS_PER_BLOCK));
    System.out.println(String.format("Current number of occupied blocks: %d", occupiedBlocks));
    System.out.println(String.format("Disk utilization: %f %%", getDiskUtilization() * 100));
    System.out.println();
  }
}