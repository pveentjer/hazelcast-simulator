package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;

public class Employee implements DataSerializable, Comparable<Employee> {

    public int id;
    public int age;
    public double salary;
    public boolean active;

    public Employee() {
    }


    public Employee(int id, int age, boolean active, double salary) {
        this.id = id;
        this.age = age;
        this.salary = salary;
        this.active = active;
    }

    public int getId() {
        return id;
    }

    public int getAge() {
        return age;
    }

    public double getSalary() {
        return salary;
    }

    public boolean isActive() {
        return active;
    }

    @Override
    public int compareTo(@Nonnull Employee employee) {
        return id - employee.id;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Employee employee = (Employee) o;
        if (id != employee.id) {
            return false;
        }
        if (age != employee.age) {
            return false;
        }
        if (active != employee.active) {
            return false;
        }
        if (Double.compare(employee.salary, salary) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + age;
        result = 31 * result + (active ? 1 : 0);
        temp = Double.doubleToLongBits(salary);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Employee{"
                + "id=" + id
                + ", age=" + age
                + ", active=" + active
                + ", salary=" + salary
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(age);
        out.writeDouble(salary);
        out.writeBoolean(active);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        age = in.readInt();
        salary = in.readDouble();
        active = in.readBoolean();
    }
}
