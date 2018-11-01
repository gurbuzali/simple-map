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

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

public class ComputeIfAbsentOperation extends AbstractKeyedSimpleMapOperation {

    private DistributedFunction mappingFunction;

    public ComputeIfAbsentOperation() {
    }

    public ComputeIfAbsentOperation(String name, Data key, DistributedFunction mappingFunction) {
        super(name, key);
        this.mappingFunction = mappingFunction;
    }

    @Override
    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        response = getMap().computeIfAbsent(key, keyData -> {
            Object keyObject = nodeEngine.toObject(keyData);
            return nodeEngine.toData(mappingFunction.apply(keyObject));
        });
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mappingFunction);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mappingFunction = in.readObject();
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.COMPUTE_IF_ABSENT;
    }
}
