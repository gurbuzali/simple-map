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
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PutAllOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private Map<Integer, Map<Data, Data>> partitionedMap;

    public PutAllOperationFactory() {
    }

    public PutAllOperationFactory(String name, Map<Integer, Map<Data, Data>> partitionedMap) {
        this.name = name;
        this.partitionedMap = partitionedMap;
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        return new PutAllOperation(name, partitionedMap.get(partition));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionedMap.size());
        for (Map.Entry<Integer, Map<Data, Data>> entry : partitionedMap.entrySet()) {
            out.writeInt(entry.getKey());
            Util.writeMap(out, entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        int size = in.readInt();
        partitionedMap = new ConcurrentHashMap<>(size);
        for (int i = 0; i < size; i++) {
            partitionedMap.put(in.readInt(), Util.readMap(in));
        }
    }

    @Override
    public int getFactoryId() {
        return SimpleMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.PUT_ALL_FACTORY;
    }


}
