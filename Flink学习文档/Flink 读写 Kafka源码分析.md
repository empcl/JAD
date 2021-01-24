# 前言

# Flink 读写Kafka原理

![flink kafka原理图](C:\Users\25211\Desktop\flink kafka原理图.jpeg)

![flink kafka原理图 -- producer](C:\Users\25211\Desktop\flink kafka原理图 -- producer.jpeg)



# Flink DataStream读写Kafka

构建FlinkKafkaConsumer

```java
FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), props);
```

1、初始化FlinkKafkaConsumerBase对象

2、设置key、value的反序列化类都是`org.apache.kafka.common.serialization.ByteArrayDeserializer`，即使用户添加了其他反序列化类设置。



构建DataStreamSource

```java
public StreamSource(SRC sourceFunction) {
    super(sourceFunction);
    this.chainingStrategy = ChainingStrategy.HEAD;
}
```



```java
public AbstractUdfStreamOperator(F userFunction) {
	this.userFunction = requireNonNull(userFunction);
	checkUdfCheckpointingPreconditions();
}
```



```java
	public DataStreamSource(StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator,
			boolean isParallel, String sourceName) {
		super(environment, new SourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}
```

1、

```java
new SimpleUdfStreamOperatorFactory<OUT>((AbstractUdfStreamOperator) operator); // operator : StreamSource
```

2、设置并行度

Flink Kafka 执行

构建SourceStreamTask

```java
	public SourceStreamTask(Environment env) {
		super(env);
		this.sourceThread = new LegacySourceFunctionThread();
	}
```



```java
// Runnable that executes the the source function in the head operator.
// private class LegacySourceFunctionThread extends Thread {
LegacySourceFunctionThread() {
	this.completionFuture = new CompletableFuture<>();
}
```

SourceStreamTask.processInput() -> sourceThread.start() -> LegacySourceFunctionThread.run()

```java
// LegacySourceFunctionThread.java
// headOperator : StreamSource
public void run() {
			try {
				headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);
				completionFuture.complete(null);
			} catch (Throwable t) {
				// Note, t can be also an InterruptedException
				completionFuture.completeExceptionally(t);
			}
		}
```

可以看到，最后执行的是StreamSource#run()方法。

```java
// StreamSource.java
public void run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final OperatorChain<?, ?> operatorChain) throws Exception {

		run(lockingObject, streamStatusMaintainer, output, operatorChain);
}

public void run(...) throws Exception {
	...
	userFunction.run(ctx);
	...
}
```

这里的userFunction，就是在之前传入的FlinkKafkaConsumer对象，换句话说，这里实际上调用的是FlinkKafkaConsumer#run()方法，也就是FlinkKafkaConsumerBase#run()方法，至此，进入了真正的kafka消费阶段。



因为 KafkaConsumer 不是线程安全的，所以每个线程都需要生成独立的 KafkaConsumer 对象，即 this.consumer = getConsumer(kafkaProperties)。



### 动态分区识别的实现





# Flink SQL方式读写Kafka



------------------------------------------

ResultTypeQueryable：获得当前function或者input format的输出数据字段类型。通过提供的getProducedType()方法，可以避免对当前function或者input format进行反射操作来获得输出数据字段类型，并且当在数据类型可能会随着参数传入的变化而变化的情况下就会很有用。以JsonRowDeserializationSchema为例，数据输出类型typeInfo，根据其在构造时候指定的情况来定。

AbstractFetcher：该类主要包含两种作用：

* 实现连接Kafka brokers，从kafka分区中拉取数据。
* 实现发射数据和追踪offset的逻辑，以及可选的时间戳分配和watermark生成功能。

Handover：hand over（切换）。

KafkaTopicPartition：Kafka topic中分区的flink description，实现了Serializable接口。

```java
private final String topic;
private final int partition;
private final int cachedHash;
```

KafkaTopicPartitionState：Flink Kafka Consumer 对每个Kafka分区持有的状态，包括队每个分区的kafka descriptor。这个类描述了每个分区的offset 状态。当然，其子类（KafkaTopicPartitionStateWithPeriodicWatermarks、KafkaTopicPartitionStateWithPunctuatedWatermarks）提供了更加具体的状态信息，即：当前watermark与抽取的时间戳等信息。

```java
/** The Flink description of a Kafka partition. */
private final KafkaTopicPartition partition;

/** The Kafka description of a Kafka partition (varies across different Kafka versions). */
private final KPH kafkaPartitionHandle;

/** The offset within the Kafka partition that we already processed. */
/** The current offset in the partition. This refers to the offset last element that
	we retrieved and emitted successfully. It is the offset that should be stored in
	a checkpoint. */
private volatile long offset;

/** The offset of the Kafka partition that has been committed. */
private volatile long committedOffset;

public KafkaTopicPartitionState(KafkaTopicPartition partition, KPH kafkaPartitionHandle) {
	this.partition = partition;
	this.kafkaPartitionHandle = kafkaPartitionHandle;
	this.offset = KafkaTopicPartitionStateSentinel.OFFSET_NOT_SET;
	this.committedOffset = KafkaTopicPartitionStateSentinel.OFFSET_NOT_SET;
}
```

KafkaTopicPartitionLeader：序列化KafkaTopicPartition以及leader节点等信息。

```java
private final int leaderId;
private final int leaderPort;
private final String leaderHost;
private final KafkaTopicPartition topicPartition;
private final int cachedHash;
```

KafkaTopicPartitionAssigner：用于分配Kafka分区给consumer subtask。

```java
public static int assign(KafkaTopicPartition partition, int numParallelSubtasks) {
	int startIndex = ((partition.getTopic().hashCode() * 31) & 0x7FFFFFFF) % numParallelSubtasks;
		
	// here, the assumption is that the id of Kafka partitions are always ascending
	// starting from 0, and therefore can be used directly as the offset clockwise from the start index
	return (startIndex + partition.getPartition()) % numParallelSubtasks;
}
```



测试：设置不开setcommitoffsetoncheckpoint会怎么样？

```text
        // 确保当偏移量的提交模式为ON_CHECKPOINTS(条件1：开启checkpoint，条件2：consumer.setCommitOffsetsOnCheckpoints(true))时，禁用自动提交
        // 该方法为父类(FlinkKafkaConsumerBase)的静态方法
        // 这将覆盖用户在properties中配置的任何设置
        // 当offset的模式为ON_CHECKPOINTS，或者为DISABLED时，会将用户配置的properties属性进行覆盖
        // 具体是将ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"的值重置为"false
        // 可以理解为：如果开启了checkpoint，并且设置了consumer.setCommitOffsetsOnCheckpoints(true)，默认为true，
        // 就会将kafka properties的enable.auto.commit强制置为false
        adjustAutoCommitConfig(properties, offsetCommitMode);
```

FlinkKafkaInternalProducer：

FlinkkafkaPartitioner：

FlinkFixedPartitioner：





=======================================================================================================

```text
  public FlinkKafkaConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
        this(null, subscriptionPattern, new KafkaDeserializationSchemaWrapper<>(valueDeserializer), props);
    }
```

实际的生产环境中可能有这样一些需求，比如有一个flink作业需要将多种不同的数据聚合到一起，而这些数据对应着不同的kafka topic，随着业务增长，新增一类数据，同时新增了一个kafka topic，如何在不重启作业的情况下作业自动感知新的topic。首先需要在构建FlinkKafkaConsumer时的properties中设置flink.partition-discovery.interval-millis参数为非负值，表示开启动态发现的开关，以及设置的时间间隔。此时FLinkKafkaConsumer内部会启动一个单独的线程定期去kafka获取最新的meta信息。具体的调用执行信息，参见下面的私有构造方法





flink 开启 cp + .setcommitoffsetoncheckpoint(true)，提交位点，状态会记录，kafka的zk中也会记录

开启cp + setcommitoffsetoncheckpoint(false)，一个任务消费完后，另一个消费的话，是否会消费之前的数据？