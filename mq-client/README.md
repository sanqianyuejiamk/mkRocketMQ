# mq-client
---
1. RocketMQ发送消息-pull方式
2. RocketMQ发送消息-push方式
3. 记录消费该条消息msgId的消费者的consumer ip
4. 添加redis开关，监控broker消息
5. topic下的redis未消费msgId，定期重发



---
1. consumer可以指定某个ip去消费消息
2. RocketMQ实现主备自动切换，zookeeper分布式锁
3. RocketMQ发送消息-顺序方式
