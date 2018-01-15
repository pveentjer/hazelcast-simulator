package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.function.Supplier;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

class EmployeeSupplier implements Supplier<Employee>, DataSerializable {
    private int maxAge;
    private double maxSalary;

    // the instance is cached to reduce litter. The dataset will write the object to offheap but will not hold on to the
    // instance.
    private final Employee employee = new Employee();

    public EmployeeSupplier() {
    }

    public EmployeeSupplier(int maxAge, double maxSalary) {
        this.maxAge = maxAge;
        this.maxSalary = maxSalary;
    }

    @Override
    public Employee get() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        employee.id = random.nextInt();
        employee.age = random.nextInt(maxAge);
        employee.active = random.nextBoolean();
        employee.salary = random.nextDouble() * maxSalary;
        return employee;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(maxAge);
        out.writeDouble(maxSalary);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        System.out.println("EmployeeSupplier dematerialized");
        this.maxAge = in.readInt();
        this.maxSalary = in.readDouble();
    }
}
