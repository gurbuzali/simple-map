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
import com.hazelcast.projectx.simplemap.operation.SetOperationFactory.SetOperationType;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class SetOperation extends AbstractSimpleMapOperation {

    private SetOperationType setOperationType;

    SetOperation() {
    }

    SetOperation(String name, SetOperationType setOperationType) {
        super(name);
        this.setOperationType = setOperationType;
    }

    @Override
    public void run() {
        response = getSet();
    }

    private Object getSet() {
        ConcurrentMap<Data, Data> map = getMap();
        switch (setOperationType) {
            case KEY:
                return map.keySet();
            case VALUE:
                return map.values();
            case ENTRY:
                return map.entrySet();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(setOperationType.name());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        setOperationType = SetOperationType.valueOf(in.readUTF());
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.SET;
    }
}
