package com.linkedin.venice.meta;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionerConfigImpl implements PartitionerConfig {
  private final StorePartitionerConfig partitionerConfig;

  public PartitionerConfigImpl(
      @JsonProperty("partitionerClass") String partitionerClass,
      @JsonProperty("partitionerParams") Map<String, String> partitionerParams,
      @JsonProperty("amplificationFactor") int amplificationFactor) {
    this.partitionerConfig = new StorePartitionerConfig();
    this.partitionerConfig.partitionerClass = partitionerClass;
    /**
     * TODO: once the full stack migrates to adopt modern avro versions (1.7+), we could specify `java.string` in schema,
     * then this kind of conversion can be avoided.
     */
    this.partitionerConfig.partitionerParams = Utils.convertStringMapToCharSequenceMap(partitionerParams);
    this.partitionerConfig.amplificationFactor = amplificationFactor;
  }

  PartitionerConfigImpl(StorePartitionerConfig partitionerConfig) {
    this.partitionerConfig = partitionerConfig;
  }

  public PartitionerConfigImpl() {
    this(DefaultVenicePartitioner.class.getName(), new HashMap<>(), 1);
  }

  @Override
  public String getPartitionerClass() {
    return this.partitionerConfig.partitionerClass.toString();
  }

  @Override
  public Map<String, String> getPartitionerParams() {
    return Utils.convertCharSequenceMapToStringMap(this.partitionerConfig.partitionerParams);
  }

  @Override
  public int getAmplificationFactor() {
    return this.partitionerConfig.amplificationFactor;
  }

  @Override
  public void setAmplificationFactor(int amplificationFactor) {
    this.partitionerConfig.amplificationFactor = amplificationFactor;
  }

  @Override
  public void setPartitionerClass(String partitionerClass) {
    this.partitionerConfig.partitionerClass = partitionerClass;
  }

  @Override
  public void setPartitionerParams(Map<String, String> partitionerParams) {
    this.partitionerConfig.partitionerParams = Utils.convertStringMapToCharSequenceMap(partitionerParams);
  }

  @Override
  public StorePartitionerConfig dataModel() {
    return this.partitionerConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionerConfigImpl that = (PartitionerConfigImpl) o;
    return AvroCompatibilityUtils.compare(partitionerConfig, that.partitionerConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionerConfig);
  }

  @JsonIgnore
  public PartitionerConfig clone(){
    return new PartitionerConfigImpl(getPartitionerClass(), getPartitionerParams(), getAmplificationFactor());
  }
}