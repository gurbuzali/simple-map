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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projectx.simplemap.operation.ReplicateOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SimpleMapService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "projectX:simpleMap";

    private final NodeEngine nodeEngine;

    private PartitionContainer[] partitionContainers;

    public SimpleMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.partitionContainers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer();
        }
    }

    public void reset() {
        partitionContainers = null;
    }

    public void shutdown(boolean terminate) {
        reset();
    }

    public DistributedObject createDistributedObject(String name) {
        return new SimpleMapProxy<>(name, nodeEngine, this);
    }

    public void destroyDistributedObject(String name) {
        for (PartitionContainer partitionContainer : partitionContainers) {
            partitionContainer.mapContainers.remove(name);
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }
        return new ReplicateOperation(partitionContainers[event.getPartitionId()].mapContainers);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            cleanPartition(event);
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            cleanPartition(event);
        }
    }

    private void cleanPartition(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        int newReplicaIndex = event.getNewReplicaIndex();
        if (newReplicaIndex < 0 || newReplicaIndex > 1) {
            partitionContainers[partitionId].mapContainers.clear();
        }
    }

    public ConcurrentMap<Data, Data> getOrCreateMap(int partitionId, String name) {
        ConcurrentMap<String, ConcurrentMap<Data, Data>> mapContainers = partitionContainers[partitionId].mapContainers;
        return mapContainers.computeIfAbsent(name, key -> new ConcurrentHashMap<>());
    }

    public void insertMigrationData(int partitionId, ConcurrentMap<String, ConcurrentMap<Data, Data>> mapContainers) {
        partitionContainers[partitionId].mapContainers = mapContainers;
    }
}
