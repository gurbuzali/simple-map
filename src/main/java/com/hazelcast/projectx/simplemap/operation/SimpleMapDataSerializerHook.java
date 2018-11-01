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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public class SimpleMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = 808;

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int REMOVE = 2;
    public static final int COMPUTE = 3;
    public static final int COMPUTE_IF_ABSENT = 4;
    public static final int SIZE = 5;
    public static final int SIZE_FACTORY = 6;
    public static final int REPLICATE = 7;
    public static final int CONTAINS = 8;
    public static final int CONTAINS_VALUE = 9;
    public static final int CLEAR = 10;
    public static final int CLEAR_FACTORY = 11;
    public static final int REPLACE = 12;
    public static final int MERGE = 13;
    public static final int FOR_EACH = 14;
    public static final int FOR_EACH_FACTORY = 15;
    public static final int REPLACE_ALL = 16;
    public static final int REPLACE_ALL_FACTORY = 17;
    public static final int PUT_ALL = 18;
    public static final int PUT_ALL_FACTORY = 19;
    public static final int SET = 20;
    public static final int SET_FACTORY = 21;
    private static final int LEN = SET_FACTORY + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[PUT] = arg -> new PutOperation();
        constructors[GET] = arg -> new GetOperation();
        constructors[REMOVE] = arg -> new RemoveOperation();
        constructors[COMPUTE] = arg -> new ComputeOperation();
        constructors[COMPUTE_IF_ABSENT] = arg -> new ComputeIfAbsentOperation();
        constructors[SIZE] = arg -> new SizeOperation();
        constructors[SIZE_FACTORY] = arg -> new SizeOperationFactory();
        constructors[REPLICATE] = arg -> new ReplicateOperation();
        constructors[CONTAINS] = arg -> new ContainsOperation();
        constructors[CONTAINS_VALUE] = arg -> new ContainsValueOperationFactory();
        constructors[CLEAR] = arg -> new ClearOperation();
        constructors[CLEAR_FACTORY] = arg -> new ClearOperationFactory();
        constructors[REPLACE] = arg -> new ReplaceOperation();
        constructors[MERGE] = arg -> new MergeOperation();
        constructors[FOR_EACH] = arg -> new ForEachOperation();
        constructors[FOR_EACH_FACTORY] = arg -> new ForEachOperationFactory();
        constructors[REPLACE_ALL] = arg -> new ReplaceAllOperation();
        constructors[REPLACE_ALL_FACTORY] = arg -> new ReplaceAllOperationFactory();
        constructors[PUT_ALL] = arg -> new PutAllOperation();
        constructors[PUT_ALL_FACTORY] = arg -> new PutAllOperationFactory();
        constructors[SET] = arg -> new SetOperation();
        constructors[SET_FACTORY] = arg -> new SetOperationFactory();

        return new ArrayDataSerializableFactory(constructors);
    }
}
