package com.phone.analystic.modle.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PlatformDimension extends BaseDimension{

    private int id;
    private String platformName;

    public PlatformDimension(){

    }
    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }
    public PlatformDimension(int id, String platformName) {
        this(platformName);
        this.id = id;
    }

    public static PlatformDimension getInstance(String platformName) {

        String pl = StringUtils.isEmpty(platformName) ? GlobalConstants.DEFAULT_VALUE : platformName;

        return new PlatformDimension(pl);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName = in.readUTF();
    }


    /**
     * 构建平台维度的集合对象
     * @param platformName
     * @return
     */
    public static List<PlatformDimension> buildList(String platformName){
        if(StringUtils.isEmpty(platformName)){
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatformDimension> li = new ArrayList<PlatformDimension>();
        li.add(new PlatformDimension(platformName));
        li.add(new PlatformDimension(GlobalConstants.ALL_OF_VALUE));
        return li;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return  tmp;
        }
        return this.platformName.compareTo(other.platformName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

}
