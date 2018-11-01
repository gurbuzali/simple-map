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

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public class ComputeOperation extends AbstractKeyedSimpleMapOperation {

    private DistributedBiFunction mappingFunction;
    private boolean ifPresent;

    public ComputeOperation() {
    }

    public ComputeOperation(String name, Data key, DistributedBiFunction mappingFunction, boolean ifPresent) {
        super(name, key);
        this.mappingFunction = mappingFunction;
        this.ifPresent = ifPresent;
    }

    @Override
    public void run() {
        BiFunction<Data, Data, Data> wrapperFunction = wrapperFunction();
        ConcurrentMap<Data, Data> map = getMap();
        response = ifPresent ? map.computeIfPresent(key, wrapperFunction) : map.compute(key, wrapperFunction);
    }

    private BiFunction<Data, Data, Data> wrapperFunction() {
        NodeEngine nodeEngine = getNodeEngine();
        return (keyData, valueData) -> {
            Object keyObject = nodeEngine.toObject(keyData);
            Object valueObject = nodeEngine.toObject(valueData);
            return nodeEngine.toData(mappingFunction.apply(keyObject, valueObject));
        };
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mappingFunction);
        out.writeBoolean(ifPresent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mappingFunction = in.readObject();
        ifPresent = in.readBoolean();
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.COMPUTE;
    }
}
