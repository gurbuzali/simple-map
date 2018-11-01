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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.projectx.simplemap.SimpleMapService;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReplicateOperation extends Operation implements IdentifiedDataSerializable {

    private ConcurrentMap<String, ConcurrentMap<Data, Data>> mapContainers;

    public ReplicateOperation() {
    }

    public ReplicateOperation(ConcurrentMap<String, ConcurrentMap<Data, Data>> mapContainers) {
        this.mapContainers = mapContainers;
    }

    @Override
    public void run() {
        SimpleMapService service = getService();
        service.insertMigrationData(getPartitionId(), mapContainers);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(mapContainers.size());
        for (Map.Entry<String, ConcurrentMap<Data, Data>> entry : mapContainers.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        mapContainers = new ConcurrentHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            ConcurrentMap<Data, Data> map = in.readObject();
            mapContainers.put(name, map);
        }

    }

    @Override
    public String getServiceName() {
        return SimpleMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SimpleMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.REPLICATE;
    }
}
