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

public class ContainsOperation extends AbstractKeyedSimpleMapOperation {

    private boolean isValue;

    public ContainsOperation() {
    }

    public ContainsOperation(String name, Data key) {
        this(name, key, false);
    }

    public ContainsOperation(String name, Data key, boolean isValue) {
        super(name, key);
        this.isValue = isValue;
    }

    @Override
    public void run() {
        ConcurrentMap<Data, Data> map = getMap();
        response = isValue ? map.containsValue(key) : map.containsKey(key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(isValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        isValue = in.readBoolean();
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.CONTAINS;
    }
}
