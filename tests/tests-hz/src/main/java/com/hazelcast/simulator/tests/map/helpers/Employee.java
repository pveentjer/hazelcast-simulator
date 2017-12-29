/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.helpers;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Random;

public class Employee implements Serializable, Comparable<Employee> {

    public static final int MAX_AGE = 75;
    public static final double MAX_SALARY = 1000.0;

    private static final Random RANDOM = new Random();

    public int id;
    public int age;
    public double salary;
    public boolean active;

    public Employee() {
    }

    public Employee(int id) {
        this.id = id;
        randomizeProperties();
    }

    public Employee(int id,int age, boolean active, double salary) {
        this.id = id;
        this.age = age;
        this.salary = salary;
        this.active = active;
    }

    public final void randomizeProperties() {
        age = RANDOM.nextInt(MAX_AGE);
        salary = RANDOM.nextDouble() * MAX_SALARY;
        active = RANDOM.nextBoolean();
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
}
