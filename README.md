# simple-map
Hazelcast Simple Map

How to use

```
        ServiceConfig serviceConfig = new ServiceConfig()
                .setName(SimpleMapService.SERVICE_NAME)
                .setEnabled(true)
                .setClassName(SimpleMapService.class.getName());

        Config config = new Config();
        config.getServicesConfig().addServiceConfig(serviceConfig);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        Map<String, String> map = instance.getDistributedObject(SimpleMapService.SERVICE_NAME, "map");
        
        map.put("key1", "val1");
        map.get("key1");
        
```
