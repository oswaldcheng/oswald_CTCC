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
 * 时间维度
 *
 * @ClassName DateDimension
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class DateDimension extends BaseDimension {
    //年
    private String year;
    //月
    private String month;
    //日
    private String day;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateDimension that = (DateDimension) o;

        if (year != null ? !year.equals(that.year) : that.year != null) {
            return false;
        }
        if (month != null ? !month.equals(that.month) : that.month != null) {
            return false;
        }
        return day != null ? day.equals(that.day) : that.day == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension anotherDateDimension = (DateDimension) o;
        int result = this.year.compareTo(anotherDateDimension.year);
        if (result != 0) return result;

        result = this.month.compareTo(anotherDateDimension.month);
        if (result != 0) return result;
        result = this.day.compareTo(anotherDateDimension.day);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
    }
}
