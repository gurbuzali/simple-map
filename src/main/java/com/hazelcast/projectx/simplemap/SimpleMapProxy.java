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

package com.hazelcast.projectx.simplemap;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projectx.simplemap.operation.ClearOperationFactory;
import com.hazelcast.projectx.simplemap.operation.ComputeIfAbsentOperation;
import com.hazelcast.projectx.simplemap.operation.ComputeOperation;
import com.hazelcast.projectx.simplemap.operation.ContainsOperation;
import com.hazelcast.projectx.simplemap.operation.ContainsValueOperationFactory;
import com.hazelcast.projectx.simplemap.operation.ForEachOperationFactory;
import com.hazelcast.projectx.simplemap.operation.GetOperation;
import com.hazelcast.projectx.simplemap.operation.MergeOperation;
import com.hazelcast.projectx.simplemap.operation.PutAllOperationFactory;
import com.hazelcast.projectx.simplemap.operation.PutOperation;
import com.hazelcast.projectx.simplemap.operation.RemoveOperation;
import com.hazelcast.projectx.simplemap.operation.ReplaceAllOperationFactory;
import com.hazelcast.projectx.simplemap.operation.ReplaceOperation;
import com.hazelcast.projectx.simplemap.operation.SetOperationFactory;
import com.hazelcast.projectx.simplemap.operation.SetOperationFactory.SetOperationType;
import com.hazelcast.projectx.simplemap.operation.SizeOperationFactory;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.projectx.simplemap.SimpleMapService.SERVICE_NAME;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class SimpleMapProxy<K, V> extends AbstractDistributedObject<SimpleMapService> implements Map<K, V> {

    private final String name;

    SimpleMapProxy(String name, NodeEngine nodeEngine, SimpleMapService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    public V put(K key, V value) {
        return putInternal(key, value, false);
    }

    public V putIfAbsent(K key, V value) {
        return putInternal(key, value, true);
    }

    public V get(Object key) {
        Data keyData = toData(key);
        Operation operation = new GetOperation(name, keyData);
        return invokeOnPartition(operation, keyData);
    }

    public V getOrDefault(Object key, V defaultValue) {
        V value = get(key);
        return value == null ? defaultValue : value;
    }

    public V remove(Object key) {
        Data keyData = toData(key);
        Operation operation = new RemoveOperation(name, keyData);
        return invokeOnPartition(operation, keyData);
    }

    public boolean remove(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        Operation operation = new RemoveOperation(name, keyData, valueData);
        return invokeOnPartition(operation, keyData);
    }

    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
        return computeInternal(key, mappingFunction, false);
    }

    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        Data keyData = toData(key);
        Operation operation = new ComputeIfAbsentOperation(name, keyData, (DistributedFunction) mappingFunction);
        return invokeOnPartition(operation, keyData);
    }

    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
        return computeInternal(key, mappingFunction, true);
    }

    public int size() {
        OperationService operationService = getNodeEngine().getOperationService();
        try {
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            return resultMap.values().stream().mapToInt(o -> (int) o).sum();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean containsKey(Object key) {
        Data keyData = toData(key);
        Operation operation = new ContainsOperation(name, keyData);
        return invokeOnPartition(operation, keyData);
    }

    public boolean containsValue(Object value) {
        OperationService operationService = getNodeEngine().getOperationService();
        Data valueData = toData(value);
        try {
            Map<Integer, Object> resultMap = operationService
                    .invokeOnAllPartitions(SERVICE_NAME, new ContainsValueOperationFactory(name, valueData));
            return resultMap.values().stream().anyMatch(TRUE::equals);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void clear() {
        OperationService operationService = getNodeEngine().getOperationService();
        try {
            operationService.invokeOnAllPartitions(SERVICE_NAME, new ClearOperationFactory(name));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean replace(K key, V oldValue, V newValue) {
        return replaceInternal(key, oldValue, newValue);
    }

    public V replace(K key, V value) {
        return replaceInternal(key, value, null);
    }

    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> mappingFunction) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        MergeOperation operation = new MergeOperation(name, keyData, valueData, (DistributedBiFunction) mappingFunction);
        return invokeOnPartition(operation, keyData);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        NodeEngine nodeEngine = getNodeEngine();
        Map<Integer, Map<Data, Data>> partitionMap = m
                .entrySet()
                .stream()
                .map(e -> entry(toData(e.getKey()), toData(e.getValue())))
                .collect(
                        groupingBy(e -> getPartitionId(e.getKey()),
                                toMap(Entry::getKey, Entry::getValue)
                        )
                );

        OperationService operationService = nodeEngine.getOperationService();
        OperationFactory operationFactory = new PutAllOperationFactory(name, partitionMap);
        try {
            operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitionMap.keySet());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public Set<K> keySet() {
        return (Set<K>) setInternal(SetOperationType.KEY);
    }

    public Collection<V> values() {
        return (Collection<V>) setInternal(SetOperationType.VALUE);
    }

    public Set<Entry<K, V>> entrySet() {
        return (Set<Entry<K, V>>) setInternal(SetOperationType.ENTRY);
    }

    private Collection setInternal(SetOperationType setOperationType) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        try {
            SetOperationFactory operationFactory = new SetOperationFactory(name, setOperationType);
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
            List<Object> objects = resultMap.values()
                                            .stream()
                                            .map(o -> (Collection) o)
                                            .collect(ArrayList::new, List::addAll, List::addAll);
            return new ResultSetWrapper<>(getNodeEngine().getSerializationService(), objects.toArray(), setOperationType);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void forEach(BiConsumer<? super K, ? super V> action) {
        OperationService operationService = getNodeEngine().getOperationService();
        try {
            OperationFactory operationFactory = new ForEachOperationFactory(name, (DistributedBiConsumer) action);
            operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        OperationService operationService = getNodeEngine().getOperationService();
        try {
            OperationFactory operationFactory = new ReplaceAllOperationFactory(name, (DistributedBiFunction) function);
            operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private <T> T replaceInternal(K key, V oldValue, V newValue) {
        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        Operation operation = new ReplaceOperation(name, keyData, oldValueData, newValueData);
        return invokeOnPartition(operation, keyData);
    }

    private V putInternal(K key, V value, boolean ifAbsent) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        Operation operation = new PutOperation(name, keyData, valueData, ifAbsent);
        return invokeOnPartition(operation, keyData);
    }

    private V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, boolean ifPresent) {
        Data keyData = toData(key);
        Operation operation = new ComputeOperation(name, keyData, (DistributedBiFunction) mappingFunction, ifPresent);
        return invokeOnPartition(operation, keyData);
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    private <T> T invokeOnPartition(Operation operation, Data key) {
        try {
            operation.setPartitionId(getPartitionId(key));
            return getNodeEngine().toObject(invokeOnPartition(operation).get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
