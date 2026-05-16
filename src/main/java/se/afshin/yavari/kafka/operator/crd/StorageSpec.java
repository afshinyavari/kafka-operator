package se.afshin.yavari.kafka.operator.crd;

public class StorageSpec {

    private String size = "10Gi";
    private String storageClassName;

    public String getSize() { return size; }
    public void setSize(String size) { this.size = size; }

    public String getStorageClassName() { return storageClassName; }
    public void setStorageClassName(String storageClassName) { this.storageClassName = storageClassName; }
}
