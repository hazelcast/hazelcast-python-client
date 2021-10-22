/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package org.example;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.org.json.JSONObject;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

/**
 * Compares map values of type {@link HazelcastJsonValue} based
 * on its {@code age} field.
 *
 * The comparator receives a boolean parameter from the client
 * to either sort the values ascending or descending order.
 */
public class AgeComparator implements Comparator<Map.Entry<Integer, HazelcastJsonValue>>, IdentifiedDataSerializable {

    private boolean reverse;

    public AgeComparator() {

    }

    public AgeComparator(boolean reverse) {
        this.reverse = reverse;
    }

    /**
     * Based on the received boolean parameter, compares the
     * {@link HazelcastJsonValue} objects according to their
     * {@code age} field.
     *
     * The comparison logic is defined here and cluster members
     * will call this to sort map values.
     */
    @Override
    public int compare(Map.Entry<Integer, HazelcastJsonValue> o1, Map.Entry<Integer, HazelcastJsonValue> o2) {
        int age1 = new JSONObject(o1.getValue().toString()).getInt("age");
        int age2 = new JSONObject(o2.getValue().toString()).getInt("age");

        if (reverse) {
            return age2 - age1;
        }
        return age1 - age2;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(reverse);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        reverse = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 1;
    }
}
