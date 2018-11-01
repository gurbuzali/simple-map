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
import java.util.concurrent.ConcurrentMap;

public class RemoveOperation extends AbstractKeyedSimpleMapOperation {

    private Data value;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data key) {
        super(name, key);
    }

    public RemoveOperation(String name, Data key, Data value) {
        super(name, key);
        this.value = value;
    }

    @Override
    public void run() {
        ConcurrentMap<Data, Data> map = getMap();
        response = value == null ? map.remove(key) : map.remove(key, value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readData();
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.REMOVE;
    }
}
