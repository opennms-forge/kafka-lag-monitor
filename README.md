# kafka-lag-monitor

This is an app which monitors kafka offsets and log size on (consumergroup, topic) and provides lag information

## compile:
```
mvn install
```

sample command:
```
java -jar target/kafka-lag-monitor
```

#### Rest API :
A simple Rest API provides lag information on  localhost:8090/topics/{topic-name}/offset

#### credits:      
This code is mostly derived from https://github.com/srotya/kafka-lag-monitor






