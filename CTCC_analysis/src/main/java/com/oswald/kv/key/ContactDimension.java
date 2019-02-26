package com.oswald.kv.key;

import com.oswald.kv.base.BaseDimension;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 联系人维度
 *
 * @ClassName ContactDimension
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class ContactDimension extends BaseDimension {
    //手机号
    private String telephone;
    //姓名
    private String name;


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ContactDimension that = (ContactDimension) o;

        if (telephone != null ? !telephone.equals(that.telephone) : that.telephone != null) {
            return false;
        }

        return name != null ? name.equals(that.name) : that.name == null;
    }


    @Override
    public int hashCode() {
        int result = telephone != null ? telephone.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension anotherContactDimension = (ContactDimension) o;

        int result = this.name.compareTo(anotherContactDimension.name);
        if (result != 0) {
            return result;
        }

        result = this.telephone.compareTo(anotherContactDimension.telephone);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.telephone);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.telephone = in.readUTF();
        this.name = in.readUTF();
    }
}