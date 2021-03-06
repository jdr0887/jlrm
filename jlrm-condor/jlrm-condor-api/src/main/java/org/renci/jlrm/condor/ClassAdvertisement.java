package org.renci.jlrm.condor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ClassAdvertisement", propOrder = {})
@XmlRootElement(name = "classAdvertisement")
public class ClassAdvertisement implements Cloneable {

    @XmlAttribute
    private String key;

    @XmlAttribute
    private String value;

    @XmlAttribute
    private ClassAdvertisementType type;

    public ClassAdvertisement() {
        super();
    }

    public ClassAdvertisement(String key, ClassAdvertisementType type) {
        super();
        this.key = key;
        this.type = type;
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
        return String.format("ClassAdvertisement [key=%s, value=%s, type=%s]", key, value, type);
    }

}
