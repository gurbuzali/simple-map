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

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.hazelcast.projectx.simplemap.SimpleMapService.SERVICE_NAME;
import static java.util.stream.IntStream.range;

public class PerformanceTest {

    static {
        System.setProperty("hazelcast.multicast.group", "224.18.19.20");
    }

    public static void main(String[] args) throws Exception {
        int threadCount = 5;
        int nodeCount = 1;
        int payload = 32;
        boolean simpleMap = false;

        Config config = getConfig();

        List<HazelcastInstance> instances = range(0, nodeCount)
                .mapToObj(i -> Hazelcast.newHazelcastInstance(config))
                .collect(Collectors.toList());

        Map<String, byte[]> map = getMap(instances, simpleMap);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalCount = new AtomicLong();
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            executorService.execute(() -> {
                byte[] bytes = new byte[payload];
                int count = 0;
                while (true) {
                    map.put(threadIndex + "-" + count, bytes);
                    count++;
                    if (count % 10_000 == 0) {
                        totalCount.addAndGet(10_000);
                    }
                }
            });
        }

        while (true) {
            Thread.sleep(2000);
            System.out.println(totalCount.get());
        }

    }

    static Config getConfig() {
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setName(SERVICE_NAME)
                     .setEnabled(true)
                     .setClassName(SimpleMapService.class.getName());

        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        return config;
    }

    static Map<String, byte[]> getMap(List<HazelcastInstance> instances, boolean simpleMap) {
        HazelcastInstance instance = instances.get(0);
        return simpleMap ? instance.getDistributedObject(SERVICE_NAME, "map") : instance.getMap("map");
    }

}
