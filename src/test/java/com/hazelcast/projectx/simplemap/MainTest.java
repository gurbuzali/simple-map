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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * todo add proper javadoc
 */
public class MainTest {

    static {
        System.setProperty("hazelcast.multicast.group", "224.18.19.20");
    }

    public static void main(String[] args) throws Exception {

        ServiceConfig serviceConfig = new ServiceConfig()
                .setName(SimpleMapService.SERVICE_NAME)
                .setEnabled(true)
                .setClassName(SimpleMapService.class.getName());

        Config config = new Config();
        config.getServicesConfig().addServiceConfig(serviceConfig);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        Map<String, String> map = instance.getDistributedObject(SimpleMapService.SERVICE_NAME, "map");

        map.put("key1", "val1");
        map.put("key2", "val1");

        String result = map.compute("key1", (DistributedBiFunction<String, String, String>) (key, value) -> {
            if (key.equalsIgnoreCase(value)) {
                return "equals";
            } else {
                return "notEquals";
            }
        });
        assertEquals("notEquals", result);

        result = map.computeIfPresent("key2",
                (DistributedBiFunction<String, String, String>) (key, value) -> key + "-" + value);
        assertEquals("key2-val1", result);

        result = map.computeIfAbsent("key2",
                (DistributedFunction<String, String>) key -> key + "-");
        assertEquals("key2-val1", result);

        test0(map);
        test1(map);
        test2(map);

        map.clear();

        HashMap<String, String> temp = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            temp.put("key-" + i, "val-" + i);
        }
        map.putAll(temp);
        assertEquals(10, map.size());
        assertEquals("val-7", map.get("key-7"));

        System.out.println("key-set");
        map.keySet().forEach(System.out::println);

        System.out.println("values");
        map.values().forEach(System.out::println);

        System.out.println("entry-set");
        map.entrySet().forEach(e -> System.out.println(e.getKey() + " - " + e.getValue()));
    }

    static void test0(Map<String, String> map) {
        assertEquals(2, map.size());
        map.clear();
        assertEquals(0, map.size());

        String result = map.computeIfPresent("key2",
                (DistributedBiFunction<String, String, String>) (key, value) -> key + "-" + value);
        assertNull(result);

        result = map.computeIfAbsent("key", (DistributedFunction<String, String>) key -> key + "-v");
        assertEquals("key-v", result);
        assertEquals("key-v", map.get("key"));

        assertEquals(true, map.containsKey("key"));
        assertEquals(false, map.containsKey("key1"));

        assertEquals(true, map.containsValue("key-v"));
        assertEquals(false, map.containsValue("key"));
    }

    static void test1(Map<String, String> map) {
        map.forEach((DistributedBiConsumer<String, String>) (key, value) -> {
            System.out.println("key: " + key + ", val: " + value);
        });

        String result = map.merge("key", "justVal",
                (DistributedBiFunction<String, String, String>) (oldValue, newValue) -> oldValue + "-merged-" + newValue);
        assertEquals("key-v-merged-justVal", result);
        assertEquals("key-v-merged-justVal", map.get("key"));

        result = map.merge("key1", "justVal",
                (DistributedBiFunction<String, String, String>) (key, val) -> key + "-merged-" + val);
        assertEquals("justVal", result);
        assertEquals("justVal", map.get("key1"));

        result = map.remove("key");
        assertEquals("key-v-merged-justVal", result);
        assertNull(map.get("key"));

        assertEquals(false, map.remove("key1", "no"));
        assertEquals(true, map.remove("key1", "justVal"));

        assertEquals(0, map.size());
    }

    static void test2(Map<String, String> map) {
        map.put("key1", "val1");
        map.put("key2", "val2");

        map.replaceAll((DistributedBiFunction<String, String, String>) (key, val) -> {
            if (key.equalsIgnoreCase("key1")) {
                return val + "a";
            } else {
                return val + "b";
            }
        });
        assertEquals("val1a", map.get("key1"));
        assertEquals("val2b", map.get("key2"));

        String result = map.replace("key1", "v1");
        assertEquals("val1a", result);
        assertNull(map.replace("key", "aa"));

        assertEquals(true, map.replace("key2", "val2b", "valb"));
        assertEquals(false, map.replace("key2", "val2b", "valc"));
    }

    static void assertEquals(Object expected, Object actual) {
        if (expected == null || !expected.equals(actual)) {
            throw new AssertionError("Expected: " + expected + ", Actual: " + actual);
        }
    }

    static void assertNull(Object o) {
        if (o != null) {
            throw new AssertionError("Expected null but get: " + o);
        }
    }


}
