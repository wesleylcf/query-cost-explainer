/**
 * Class for storage related configuration.
 * Implemented in the builder pattern to support overriding configuration properties easily.
 */
public class StorageConfiguration {
  /**
   * Disk utilization threshold to trigger compaction process
   */
  private float compactionThreshold;

  private StorageConfiguration(Builder builder) {
      this.compactionThreshold = builder.compactionThreshold;
  }

  public float getCompactionThreshold() {
      return compactionThreshold;
  }
  
  public static class Builder {
    private float compactionThreshold = 0.9f;

    public Builder setCompactionThreshold(float value) {
        this.compactionThreshold = value;
        return this;
    }

    public StorageConfiguration build() {
        return new StorageConfiguration(this);
    }
  }
}
