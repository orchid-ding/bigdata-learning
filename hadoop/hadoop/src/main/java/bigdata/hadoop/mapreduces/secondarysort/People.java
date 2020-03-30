package bigdata.hadoop.mapreduces.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dingchuangshi
 */

public class People implements WritableComparable<People> {

    private String name;

    private int age;

    private int salary;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "People{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", salary='" + salary + '\'' +
                '}';
    }

    @Override
    public int compareTo(People other) {
        int compare = this.salary - other.salary;
        // 如果工资相同则按照年龄升序
        // 否则 按照工资降序
        return compare == 0 ? this.age - other.age : -compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeInt(this.salary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
        this.salary = in.readInt();
    }
}
