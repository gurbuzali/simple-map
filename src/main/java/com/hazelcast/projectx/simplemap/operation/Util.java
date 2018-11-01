/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.projectx.simplemap.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class Util {

    private Util() {
    }

    public static void writeMap(ObjectDataOutput out, Map<Data, Data> map) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Data, Data> entry : map.entrySet()) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    public static Map<Data, Data> readMap(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        HashMap<Data, Data> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(in.readData(), in.readData());
        }
        return map;
    }
}
