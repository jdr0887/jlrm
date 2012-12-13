package org.renci.jlrm.condor;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ClassAdvertisement implements Cloneable {

    private String key;

    private String value;

    private ClassAdvertisementType type;

    public ClassAdvertisement() {
        super();
    }

    public ClassAdvertisement(String key, ClassAdvertisementType type, String value) {
        super();
        this.key = key;
        this.type = type;
        this.value = value;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key
     *            the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return the type
     */
    public ClassAdvertisementType getType() {
        return type;
    }

    /**
     * @param type
     *            the type to set
     */
    public void setType(ClassAdvertisementType type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClassAdvertisement other = (ClassAdvertisement) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    @Override
    public ClassAdvertisement clone() throws CloneNotSupportedException {
        return new ClassAdvertisement(this.key, this.type, this.value);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
