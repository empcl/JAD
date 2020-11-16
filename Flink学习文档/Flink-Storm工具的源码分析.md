# Flink-Storm工具源码介绍 
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flink、Storm 都是优秀的大数据实时计算引擎，具有低延迟、容错、高可用等特点。Storm 依赖 zookeeper 存储状态信息，存在着性能瓶颈问题；Storm 的ack消息保障机制也会导致吞吐量并不是很理想。Flink 是2018年开源大数据生态中发展“最快”的引擎，由于其出色的性能优势，提供低延迟、高吞吐、容错、高可用等特性，以及具有状态支持、一致性语义、丰富的窗口功能等高级特性逐渐成为各大互联网公司青睐的对象。
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flink-Storm 工具是 Flink 官方提供的用于 Flink 兼容 Storm 程序 beta 工具，并且在 Release 1.8 之后去掉相关代码。用户可以使用该工具提供的方法完成 Storm Topology 到 Flink Topology 的转换工作，然后通过 StreamExecutionEnvironment.execute() 方法将转换后的程序提交到 Flink 运行时环境中。
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flink-Storm 的源码分析工作以 Release 1.7 为标准，读者可以自行下载、阅读分析，链接：[https://github.com/apache/flink/tree/release-1.7/flink-contrib/flink-storm](https://github.com/apache/flink/tree/release-1.7/flink-contrib/flink-storm)。
## Flink-Storm工具的原理介绍
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;通过 Storm 原生 TopologyBuilder 构建好 Storm Topology。FlinkTopology.createTopology(builder) 将 StormTopology 转换为 Flink 对应的 Streaming Dataflow。SpoutWrapper 用于将 spout 转换为 RichParallelSourceFunction，spout 的outputFields转换成 source 的TypeInformation。BoltWrapper 用于将 bolt 转换成对应的 operator，其中 grouping 转换为对上游 spout/bolt 节点的 DataStream 的设置分区策略操作。构建完 FlinkTopology 之后，就可以通过 StreamExecutionEnvironment 生成 StreamGraph 获取 JobGraph，之后将 JobGraph 提交到 Flink 运行时环境。（注：该部分参考《[58 集团大规模 Storm 任务平滑迁移至 Flink 的秘密
](https://blog.csdn.net/yunqiinsight/article/details/98596301)》）
![Flink-Storm原理图](https://p9-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/ed0a593299bb404aa211870a6ba57435~tplv-k3u1fbpfcp-watermark.image)

## Flink-Storm工具的源码分析
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;为了更好的分析源码，我们从样例代码开始着手，看看程序在运行过程中，需要调用哪些方法、每个方法完成的功能以及每个变量所包含的信息都有什么。在本文中，作者以常见的WordCount程序为例，进行Flink-Storm工具的源码分析。代码逻辑很简单，初始化WordCountInMemorySpout对象作为数据源，使用BoltTokenizer对象中的逻辑对数据进行解析，并且按照word字段进行聚合操作，最后将聚合后的结果输出打印。
```
final TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("source", new WordCountInMemorySpout());
builder.setBolt("tokenierzer", new BoltTokenizer(), 2).shuffleGrouping("source");
builder.setBolt("counter", new BoltCounter(), 2)
            .fieldsGrouping("tokenierzer",new Fields("word"));
builder.setBolt("sink", new BoltPrintSink(),2).shuffleGrouping("counter");
```
### Storm拓扑到Flink拓扑的转换
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 以上述获得TopologyBuilder对象builder作为参数调用`FlinkTopology.createTopology(builder);`方法获得FlinkTopology对象flinkTopology，完成Storm拓扑到Flink拓扑的转换。然后调用`flinkTopology.execute();`方法以执行Flink程序。
```
public static FlinkTopology createTopology(TopologyBuilder stormBuilder) {
    return new FlinkTopology(stormBuilder);
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 在createTopology()方法中，实际上是根据stormBuilder来初始化FlinkTopology对象。在初始化FlinkTopology的过程中，通过`getPrivateField()`方法获得Storm程序中的spout以及bolt信息，分别赋值给spouts、bolts变量；接着，初始化StreamExecutionEnvironment对象并赋值给env变量，用于提交Flink程序；最后，调用`translateTopology()`方法完成Storm Topology到Flink Topology的转换工作。
```
private FlinkTopology(TopologyBuilder builder) {
    this.builder = builder;
    this.stormTopology = builder.createTopology();

    this.spouts = getPrivateField("_spouts");
    this.bolts = getPrivateField("_bolts");

    this.env = StreamExecutionEnvironment.getExecutionEnvironment();

    translateTopology();
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `getPrivateField()`方法主要通过反射方式从用户构建的StormBuilder对象builder中提取出_spouts以及_bolts所包含的值，并返回。以_spouts为例，返回值如下：{"source" -> WordCountInMemorySpout}
```
private <T> Map<String, T> getPrivateField(String field) {
    try {
        Field f = builder.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return copyObject((Map<String, T>) f.get(builder));
    } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException("Couldn't get " + field + " from TopologyBuilder", e);
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; FlinkTopology类中提供了`execute()`方法，用户可以在完成Storm Topology转Flink Topology后，调用该方法以执行程序。
```
public JobExecutionResult execute() throws Exception {
    return env.execute();
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`translateTopology()`方法主要完成Storm Topology 转 Flink Topology的工作。总体框架就是先遍历并转换Storm程序的spouts部分，然后再遍历转换bolts部分。
```
private void translateTopology() {
    xxxx.clear();
    ...
    for (final Entry<String, IRichSpout> spout : spouts.entrySet()) {
        ...
    }
    while (bolts.size() > 0) {
        ...
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;首先会对unprocessdInputsPerBolt、outputStreams、declarers、availableInputs变量进行clear操作，避免因为之前转换的操作对当前工作带来影响。unprocessdInputsPerBolt用于表示当前bolt与其输入流之间的映射关系，类型是`HashMap<String, Set<Entry<GlobalStreamId, Grouping>>>`，如"tokenizer" -> {HashSet("GlobalStreamId(componentId:source, streamId:default)" -> {Grouping@1800} "<Grouping shuffle:NullStruct()>")}；outputStreams用于表示spout、bolt与对应的输出字段之间的映射关系，类型是`HashMap<String, HashMap<String, Fields>>`，如"source" -> {"default" -> {Fields@2161}[sentence]}；declarers用于表示spout、bolt与对应的FlinkOutputFieldsDeclarer对象之间的映射关系，类型是`HashMap<String, FlinkOutputFieldsDeclarer>`，如："source" -> {FlinkOutputFieldsDeclarer@2167} ；availableInputs用于表示当前spout、bolt节点与该节点转换为Flink Source或者Operator节点之间的映射关系，类型是`HashMap<String, HashMap<String, DataStream<Tuple>>>`，如"source" -> {"default" -> {DataStreamSource@2178}}。
```
unprocessdInputsPerBolt.clear();
outputStreams.clear();
declarers.clear();
availableInputs.clear();
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在`translateTopology()`方法中，在对几个变量进行clear操作后，会遍历spouts，并将每个spout转为对应的Flink数据源DataStreamSource。首先根据spout获得spoutId、以及userSpout信息。初始化FlinkOutputFieldsDeclarer对象declarer，将当前spout与其输出字段之间的映射信息保存到declarer对象中的outputStreams变量中，并将该outputStreams变量赋值给sourceStreams变量。将当前spoutId与declarer对象以及对应的输出字段的映射关系分别保存到declarers以及outputStreams变量中，以供获得当前节点或者输入节点的输出字段以及输出字段类型等重要信息。
```
final String spoutId = spout.getKey();
final IRichSpout userSpout = spout.getValue();

final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
userSpout.declareOutputFields(declarer);
final HashMap<String, Fields> sourceStreams = declarer.outputStreams;
this.outputStreams.put(spoutId, sourceStreams);
declarers.put(spoutId, declarer);
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;FlinkOutputFieldsDeclarer实现了OutputFieldsDeclarer接口，重写了`declareStream()`方法，并且提供了一个成员变量outputStreams，用于保存spout、bolt与其输出字段之间的映射关系，数据类型为HashMap<String, Fields>。
```
final class FlinkOutputFieldsDeclarer implements OutputFieldsDeclarer {

    /** The declared output streams and schemas. */
    final HashMap<String, Fields> outputStreams = new HashMap<String, Fields>();

    @Override
    public void declareStream(final String streamId, final boolean direct, final Fields fields) {
        ...
        this.outputStreams.put(streamId, fields);
    }
    ...
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;紧接着，定义数据类型为`HashMap<String, DataStream<Tuple>>`的变量outputStreams，用于保存当前spout与转换的DataStreamSource之间的映射关系。需要注意的是，其与FlinkTopology的成员变量outputStreams之间的区别。接下来，会判断当前spout的输出流会流向下游bolt的个数进行不同的逻辑处理。如果当前spout只会流向下游的一个bolt时，首先以userSpout、spoutId等参数构建SpoutWrapper对象，并作为参数传给调用`addSource()`方法用以定义Flink的数据源src，其中`declarer.getOutputType(outputStreamId)`用法用以指定当前数据源的输出格式，最后将当前spout对应的数据源src保存到outputStreams变量中。如果当前spout的输出流会被切分为多条流并且流向不同的bolt时，同样的也会构建SpoutWrapper对象，并且定义Flink的数据源multiSource，只不过添加了根据指定的outStreamId的split数据流操作。根据用户Storm程序`declareOutputFields()`方法中指定的输出流对应的名字，如"min"、"max"，将当前spout的输出流切分为多条输出流，并将这个多条输出流与其对应名字之间的映射保存到outputStreams中。最后，将当前spout与上述得到的outputStreams之间的映射保存到availableInputs变量中，表示当前spout已经转为Flink的数据源DataStreamSource，并且能够为下游的bolt转为Flink的算子提供参考信息。
```
final HashMap<String, DataStream<Tuple>> outputStreams = new HashMap<String, DataStream<Tuple>>();
final DataStreamSource<?> source;

if (sourceStreams.size() == 1) {
    final SpoutWrapper<Tuple> spoutWrapperSingleOutput = new SpoutWrapper<Tuple>(userSpout, spoutId, null, null);
    spoutWrapperSingleOutput.setStormTopology(stormTopology);
    # 默认是default
    final String outputStreamId = (String) sourceStreams.keySet().toArray()[0];

    DataStreamSource<Tuple> src = env.addSource(spoutWrapperSingleOutput, spoutId,declarer.getOutputType(outputStreamId));

    outputStreams.put(outputStreamId, src);
    source = src;
} else {
    final SpoutWrapper<SplitStreamType<Tuple>> spoutWrapperMultipleOutputs = new SpoutWrapper<SplitStreamType<Tuple>>(userSpout, spoutId, null, null);
    spoutWrapperMultipleOutputs.setStormTopology(stormTopology);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    DataStreamSource<SplitStreamType<Tuple>> multiSource = env.addSource(
    spoutWrapperMultipleOutputs, spoutId,(TypeInformation) TypeExtractor.getForClass(SplitStreamType.class));

    SplitStream<SplitStreamType<Tuple>> splitSource = multiSource
        .split(new StormStreamSelector<Tuple>());
    for (String streamId : sourceStreams.keySet()) {
        SingleOutputStreamOperator<Tuple> outStream = splitSource.select(streamId)
            .map(new SplitStreamMapper<Tuple>());
        outStream.getTransformation().setOutputType(declarer.getOutputType(streamId));
        outputStreams.put(streamId, outStream);
}
source = multiSource;
}
availableInputs.put(spoutId, outputStreams);
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;这里的代码，摘选于用户定义的Spout中`declareOutputFields()`方法。在该方法中，将当前spout的输出流定义为两条数据流，下游bolt会根据当前数据流是"min"还是"max"来接受不同的数据流。
```
public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("min",new Fields("value"));
    declarer.declareStream("max",new Fields("value"));
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在将spout转为对应的Flink的数据源DataStreamSource时，需要根据当前spout的并行度设置对应的DataStreamSource的并行度，实现比较简单，如下所示。
```
final ComponentCommon common = stormTopology.get_spouts().get(spoutId).get_common();
if (common.is_set_parallelism_hint()) {
    int dop = common.get_parallelism_hint();
    source.setParallelism(dop);
} else {
    common.set_parallelism_hint(1);
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;接下来，我们看下SpoutWrapper类的框架结构。SpoutWrapper类继承了RichParallelSourceFunction类，并且提供了一个成员变量IRichSpout spout以作为IRichSpout的包装类。在SpoutWrapper的构造方法中，完成基本的变量赋值，调用`WrapperSetupHelper.getNumberOfAttributes(spout, rawOutputs)`方法获得当前spout对应的输出字段的个数。另外，也重写了`run()`方法，这个方法我们在后文介绍。
```
public final class SpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> implements StoppableFunction {
    ...
    /** The wrapped {@link IRichSpout spout}. */
    private final IRichSpout spout;
    ...
    public SpoutWrapper(final IRichSpout spout, final String name, final Collection<String> rawOutputs,final Integer numberOfInvocations) throws IllegalArgumentException {
        this.spout = spout;
        this.name = name;
        this.numberOfAttributes = WrapperSetupHelper.getNumberOfAttributes(spout, rawOutputs);
        this.numberOfInvocations = numberOfInvocations;
    }
    ...
    @Override
    public final void run(final SourceContext<OUT> ctx) throws Exception {
        ...
    }
    ...
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在SpoutWrapper的构造方法中，调用`getNumberOfAttributes()`方法获得当前spout的输出字段个数。这里我们详细看下这个方法里面干了啥。首先初始化SetupOutputFieldsDeclarer对象declarer。SetupOutputFieldsDeclarer类结构比较简单，继承于OutputFieldsDeclarer类，提供了两个重要的变量：outputStreams、outputSchemas。outputStreams，用于表示当前节点（spout/bolt）与对应的输出字段的映射关系，数据类型为`HashMap<String, Fields>`，outputSchemas，用于表示当前节点（spout/bolt）与对应的输出字段个数之间的映射关系，数据类型为`HashMap<String, Integer>`。在重写的`declareStream()`方法中，主要就是完成当前节点（spout/bolt）与输出字段信息的映射关系的保存工作。遍历当前节点（spout/bolt）的输出字段的个数，对其进行校验是否满足个数要求。
```
# WrapperSetupHelper.java
static HashMap<String, Integer> getNumberOfAttributes(final IComponent spoutOrBolt,
        final Collection<String> rawOutputs) throws IllegalArgumentException {
    final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
    spoutOrBolt.declareOutputFields(declarer);

    for (Entry<String, Integer> schema : declarer.outputSchemas.entrySet()) {
        int declaredNumberOfAttributes = schema.getValue();
        if ((declaredNumberOfAttributes < 0) || (declaredNumberOfAttributes > 25)) {
            throw new IllegalArgumentException(
                "Provided bolt declares non supported number of output attributes. Must be in range [0;25] but "
                    + "was " + declaredNumberOfAttributes);
        }

        if (rawOutputs != null && rawOutputs.contains(schema.getKey())) {
            if (declaredNumberOfAttributes != 1) {
                throw new IllegalArgumentException(
                    "Ouput type is requested to be raw type, but provided bolt declares more then one output "
                        + "attribute.");
            }
            schema.setValue(-1);
        }
    }

    return declarer.outputSchemas;
}
```
```
class SetupOutputFieldsDeclarer implements OutputFieldsDeclarer {

    HashMap<String, Fields> outputStreams = new HashMap<String, Fields>();
    HashMap<String, Integer> outputSchemas = new HashMap<String, Integer>();
    ...
    @Override
    public void declareStream(final String streamId, final boolean direct, final Fields fields) {
        ...
        this.outputStreams.put(streamId, fields);
        this.outputSchemas.put(streamId, fields.size());
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;至此，对spouts进行转换的工作已经完成。纵观整个流程来看，比较简单。遍历spouts，为每个spout创建SpoutWrapper对象，并且构建DataStreamSource对象。根据当前spout是否需要进行切分流操作，进行不同的逻辑处理。最后，为每个DataStreamSource对象设置并行度。
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;接下来，我们重点来看看bolts的转换工作是如何进行的。对bolts的遍历可能需要重复多次，因为Storm拓扑中记录的bolts中的元素本身就是乱序的，不会因为Storm节点的前后位置而呈现出先后顺序，因此，当前遍历某一个节点时，其上游节点可能还有遍历到。在这里我们采用双层while循环的方式，完成bolts的转换工作。
```
while (bolts.size() > 0) {
    if (!makeProgress) {
        ...
    } 
    final Iterator<Entry<String, IRichBolt>> boltsIterator = bolts.entrySet().iterator();
    while (boltsIterator.hasNext()) {
        ...
        Set<Entry<GlobalStreamId, Grouping>> unprocessedBoltInputs = unprocessdInputsPerBolt.get(boltId);
        ...
        final Map<GlobalStreamId, DataStream<Tuple>> inputStreams = new HashMap<>(numberOfInputs);

        for (Entry<GlobalStreamId, Grouping> input : unprocessedBoltInputs) {
            ...
            inputStreams.put(streamId, processInput(boltId, userBolt, streamId, grouping, producer));
        }

        final SingleOutputStreamOperator<?> outputStream = createOutput(boltId,userBolt, inputStreams);
        ...
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;如果Storm Topology存在问题的话，将会进入下面的代码块中。这个时候，建议检查Storm程序，看看TopologyBuilder对象是否设置有问题。
```
if (!makeProgress) {
    StringBuilder strBld = new StringBuilder();
    strBld.append("Unable to build Topology. Could not connect the following bolts:");
    ...
}    
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;使用boltsIterator变量接受Storm程序的bolts。遍历bolts，将每一个bolt转化为对应的Flink算子。首先，用bolt变量表示当前遍历的bolt信息，并获得对应的boltId以及userBolt、common等信息。从unprocessdInputsPerBolt中获得当前boltId所需要输入流信息，并赋值给变量unprocessedBoltInputs。如果遍历当前bolt时，unprocessdInputsPerBolt目前并没有记录该bolt所需要的输入流信息，即unprocessedBoltInputs为null，会从当前bolt的common变量中取得输入流信息，并将该bolt所需要的输入流信息保存到unprocessdBoltInputs变量中，以及添加到unprocessdInputsPerBolt变量中。紧接着，判断availableInputs变量中目前保存的已经转为Flink的Source、Operator等数据流是否满足当前bolt所需要的输入流。如果不满足的话，跳出内层while循环，继续寻找下一个bolt节点。如果能够满足当前bolt，则将当前bolt从boltsIterator变量中remove，继续向下执行。初始化inputStreams变量，用于保存当前bolt所需要的数据输入流信息。遍历unprocessedBoltInputs变量，从availableInputs变量中获得当前bolt的对应的Flink输入流信息，并通过`processInput()`方法对上游的Flink输入流信息设置分区策略，并将当前bolt所需要的StreamId与对应的经过分区后的Flink数据输入流之间的映射关系保存到inputStreams。将当前bolt所需要的所有Flink数据输入流经过分区处理后并保存到inputStreams变量后，调用`createOutput()`方法完成当前bolt转换为Flink Operator的操作。最后，根据用户对当前bolt设置的并行度，为对应Flink Operator算子设置并行度。
```
final Iterator<Entry<String, IRichBolt>> boltsIterator = bolts.entrySet().iterator();
while (boltsIterator.hasNext()) {
    final Entry<String, IRichBolt> bolt = boltsIterator.next();
    final String boltId = bolt.getKey();
    final IRichBolt userBolt = copyObject(bolt.getValue());

    final ComponentCommon common = stormTopology.get_bolts().get(boltId).get_common();

    Set<Entry<GlobalStreamId, Grouping>> unprocessedBoltInputs = unprocessdInputsPerBolt.get(boltId);
    if (unprocessedBoltInputs == null) {
        unprocessedBoltInputs = new HashSet<>();
        unprocessedBoltInputs.addAll(common.get_inputs().entrySet());
        unprocessdInputsPerBolt.put(boltId, unprocessedBoltInputs);
    }

    // check if all inputs are available
    final int numberOfInputs = unprocessedBoltInputs.size();
    int inputsAvailable = 0;
    for (Entry<GlobalStreamId, Grouping> entry : unprocessedBoltInputs) {
        final String producerId = entry.getKey().get_componentId();
        final String streamId = entry.getKey().get_streamId();
        final HashMap<String, DataStream<Tuple>> streams = availableInputs.get(producerId);
        if (streams != null && streams.get(streamId) != null) {
            inputsAvailable++;
        }
    }

    if (inputsAvailable != numberOfInputs) {
        // traverse other bolts first until inputs are available
        continue;
    } else {
        makeProgress = true;
        boltsIterator.remove();
    }

    final Map<GlobalStreamId, DataStream<Tuple>> inputStreams = new HashMap<>(numberOfInputs);

    // GlobalStreamId: GlobalStreamId(componentId:counter, streamId:default)
    // Grouping: <Grouping shuffle:NullStruct()>
    for (Entry<GlobalStreamId, Grouping> input : unprocessedBoltInputs) {
        final GlobalStreamId streamId = input.getKey();
        final Grouping grouping = input.getValue();
        final String producerId = streamId.get_componentId();
        final Map<String, DataStream<Tuple>> producer = availableInputs.get(producerId);
        
        inputStreams.put(streamId, processInput(boltId, userBolt, streamId, grouping, producer));
    }

    final SingleOutputStreamOperator<?> outputStream = createOutput(boltId,userBolt, inputStreams);

    if (common.is_set_parallelism_hint()) {
        int dop = common.get_parallelism_hint();
        outputStream.setParallelism(dop);
    } else {
        common.set_parallelism_hint(1);
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`processInput()`方法的实现比较简单，主要用于完成对当前bolt所需要的Flink输入流设置分区策略的操作。初始化FlinkOutputFieldsDeclarer对象，将当前bolt与其输出字段之间的映射信息保存到declarer对象中的outputStreams变量中。然后，将当前boltId与declarer对象以及对应的输出字段的映射关系分别保存到declarers以及outputStreams变量中。紧接着，获得当前bolt的Flink输入流inputStream，并根据Storm程序中指定的group方式设置inputStream的分区策略，如shuffleGrouping方式对应于Flink中的`reblance()`方法，fieldsGrouping方式对应于`keyBy()`方法，并且通过`prodDeclarer.getGroupingFieldIndexes(inputStreamId,grouping.get_fields())`方法获得聚合的字段在数据流中的位置。
```
private DataStream<Tuple> processInput(String boltId, IRichBolt userBolt,GlobalStreamId streamId, Grouping grouping,Map<String, DataStream<Tuple>> producer) {
    final String producerId = streamId.get_componentId();
    final String inputStreamId = streamId.get_streamId();

    DataStream<Tuple> inputStream = producer.get(inputStreamId);

    final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
    userBolt.declareOutputFields(declarer);

    this.declarers.put(boltId, declarer);
    this.outputStreams.put(boltId, declarer.outputStreams);

    // if producer was processed already
    if (grouping.is_set_shuffle()) {
        inputStream = inputStream.rebalance();
    } else if (grouping.is_set_fields()) {
        final List<String> fields = grouping.get_fields();
        if (fields.size() > 0) { // hash < -- > keyBy
            FlinkOutputFieldsDeclarer prodDeclarer = this.declarers.get(producerId);
            inputStream = inputStream.keyBy(prodDeclarer
                    .getGroupingFieldIndexes(inputStreamId,grouping.get_fields()));
        } else {
            inputStream = inputStream.global();
        }
    } else if (grouping.is_set_all()) {
        inputStream = inputStream.broadcast();
    } else if (!grouping.is_set_local_or_shuffle()) {
        throw new UnsupportedOperationException("Flink only supports (local-or-)shuffle, fields, all, and global grouping");
    }
    return inputStream;
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`createOutput()`方法主要用于完成当前bolt转换为Flink Operator的工作。在这个方法里面，需要考虑当前bolt输入流是否只有一个，以及输出流是否只有一个。整体代码比较复杂且比较冗余，因此这里只列出一个代码框架，具体细节在后面详细介绍。
```
private SingleOutputStreamOperator<?> createOutput(String boltId, IRichBolt bolt,Map<GlobalStreamId, DataStream<Tuple>> inputStreams) {
    Iterator<Entry<GlobalStreamId, DataStream<Tuple>>> iterator = inputStreams.entrySet().iterator();
    Entry<GlobalStreamId, DataStream<Tuple>> input1 = iterator.next();
    ...
    DataStream<StormTuple<Tuple>> mergedInputStream = null;
    while (iterator.hasNext()) {
        Entry<GlobalStreamId, DataStream<Tuple>> input2 = iterator.next();
        ...
        if (mergedInputStream == null) {
            ...
        } else { 
            ...
        }
    }
    
    final HashMap<String, Fields> boltOutputs = this.outputStreams.get(boltId);
    ...
    if (boltOutputs.size() < 2) { // single output stream or sink
        ...
        // only one input
        if (inputStreams.entrySet().size() == 1) {
            ...
        } else {
            ...
        }
        ...
    } else {
        ...
        // only one input
        if (inputStreams.entrySet().size() == 1) {
            ...
        } else {
            ...
        }
    ...
    }
    return outputStream;
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;使用iterator变量保存当前bolt所有的Flink输入流信息。变量input1记录当前bolt的一个Flink输入流信息，并通过input1获得当前bolt的Flink输入流singleInputStream。
```
Iterator<Entry<GlobalStreamId, DataStream<Tuple>>> iterator = inputStreams.entrySet().iterator();

Entry<GlobalStreamId, DataStream<Tuple>> input1 = iterator.next();
GlobalStreamId streamId1 = input1.getKey();
String inputStreamId1 = streamId1.get_streamId();
String inputComponentId1 = streamId1.get_componentId();

Fields inputSchema1 = this.outputStreams.get(inputComponentId1).get(inputStreamId1);
DataStream<Tuple> singleInputStream = input1.getValue(); 
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;如果当前bolt具有多个输入流，那么将会进入`while(iterator.hasNext())`代码块。并且使用`connect()`方法将多个数据流进行连接，记录为mergedInputStream变量，其数据类型为StormTuple。
```
DataStream<StormTuple<Tuple>> mergedInputStream = null;
while (iterator.hasNext()) {
    Entry<GlobalStreamId, DataStream<Tuple>> input2 = iterator.next();
    GlobalStreamId streamId2 = input2.getKey();
    DataStream<Tuple> inputStream2 = input2.getValue();

    if (mergedInputStream == null) {
        mergedInputStream = singleInputStream.connect(inputStream2)
                .flatMap(new TwoFlinkStreamsMerger(streamId1, inputSchema1,streamId2, 
                                this.outputStreams.get(streamId2.get_componentId())
                                    .get(streamId2.get_streamId())))
                .returns(StormTuple.class);
    } else {
        mergedInputStream = mergedInputStream.connect(inputStream2)
                .flatMap(new StormFlinkStreamMerger(streamId2, 
                                this.outputStreams.get(streamId2.get_componentId())
                                    .get(streamId2.get_streamId())))
                .returns(StormTuple.class);
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;从outputStreams、declarers变量中获得当前bolt对应的输出字段信息以及FlinkOutputFieldsDeclarer对象declarer。使用outputStreams变量表示当前bolt对应的Flink Operator算子。在进行转换之前，会判断当前bolt会有几个输出流。我们首先来看，只有一条输出流或者没有定义输出流的情况。通过`declarer.getOutputType(outputStreamId)`方法获得当前bolt对应的Flink数据流输出类型outType。然后，根据当前bolt所需要的数据流节点个数进行不同的逻辑处理。如果只有一个输入流，首先会初始化BoltWrapper对象boltWrapper，`singleInputStream.transform(boltId, outType, boltWrapper)`方法将singleInputStream数据流的数据经过boltWrapper对象中的方式进行处理，得到当前bolt对应的Flink Operator算子outStream。如果有多条输入流，首先会初始化MergedInputsBoltWrapper对象boltWrapper，`mergedInputStream.transform(boltId, outType, boltWrapper)`方法将mergedInputStream数据流的数据经过boltWrapper对象中的方式进行处理，得到当前bolt对应的Flink Operator算子outStream。如果当前bolt下游仍有节点bolt，即`outType != null`，将boltId与当前bolt转换的Flink Operator算子的对应关系保存到availableInputs变量中。
```
final HashMap<String, Fields> boltOutputs = this.outputStreams.get(boltId);
final FlinkOutputFieldsDeclarer declarer = this.declarers.get(boltId);

final SingleOutputStreamOperator<?> outputStream;

if (boltOutputs.size() < 2) { // single output stream or sink
    String outputStreamId;
    if (boltOutputs.size() == 1) {
        outputStreamId = (String) boltOutputs.keySet().toArray()[0]; // 默认是default
    } else {
        // sink
        outputStreamId = null;
    }

    final TypeInformation<Tuple> outType = declarer.getOutputType(outputStreamId);

    final SingleOutputStreamOperator<Tuple> outStream;

    // only one input
    if (inputStreams.entrySet().size() == 1) {
        BoltWrapper<Tuple, Tuple> boltWrapper = new BoltWrapper<>(bolt, boltId,inputStreamId1, inputComponentId1, inputSchema1, null);
        boltWrapper.setStormTopology(stormTopology);
        outStream = singleInputStream.transform(boltId, outType, boltWrapper);
    } else {
        MergedInputsBoltWrapper<Tuple, Tuple> boltWrapper = new MergedInputsBoltWrapper<Tuple, Tuple>(bolt, boltId, null);
        boltWrapper.setStormTopology(stormTopology);
        outStream = mergedInputStream.transform(boltId, outType, boltWrapper);
    }

    if (outType != null) {
        // only for non-sink nodes
        final HashMap<String, DataStream<Tuple>> op = new HashMap<>();
        op.put(outputStreamId, outStream);
        availableInputs.put(boltId, op); 
    }
    outputStream = outStream;
} 
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;接着，我们再来看看当前bolt如果有多条输出流的情况。首先获得当前bolt对应的Flink数据流输出类型outType。然后，根据当前bolt所需要的数据流节点个数进行不同的逻辑处理。如果只有一个输入流，首先会初始化BoltWrapper对象boltWrapperMultipleOutputs，`singleInputStream.transform(boltId, outType, boltWrapperMultipleOutputs)`方法将singleInputStream数据流的数据经过boltWrapperMultipleOutputs对象中的方式进行处理，得到当前bolt对应的Flink Operator算子outStream。如果有多条输入流，首先会初始化MergedInputsBoltWrapper对象boltWrapperMultipleOutputs，`mergedInputStream.transform(boltId, outType, boltWrapperMultipleOutputs)`方法将mergedInputStream数据流的数据经过boltWrapper对象中的方式进行处理，得到当前bolt对应的Flink Operator算子outStream。因为当前bolt有多条输出流，需要对转换的Flink数据流做split切分操作，将该boltId的每个输出流名称outputStreamId与其对应的Flink Operator算子的对应关系保存到`HashMap<String, DataStream<Tuple>> op`变量中，另外，也会将当前boltId与上述得到的op变量的映射关系保存到availableInputs变量中。读者在对具有多个输出流的spout、bolt进行转换后，观察Flink拓扑图时，会发现莫名其妙的多处很多个map算子，其实这个map算子的产生是因为`splitStream.select(outputStreamId).map(new SplitStreamMapper<Tuple>());`这条语句。
```
final TypeInformation<SplitStreamType<Tuple>> outType = (TypeInformation) TypeExtractor.getForClass(SplitStreamType.class);

final SingleOutputStreamOperator<SplitStreamType<Tuple>> multiStream;

// only one input
if (inputStreams.entrySet().size() == 1) {
    final BoltWrapper<Tuple, SplitStreamType<Tuple>> boltWrapperMultipleOutputs = new BoltWrapper<>(bolt, boltId, inputStreamId1, inputComponentId1, inputSchema1, null);
    boltWrapperMultipleOutputs.setStormTopology(stormTopology);
    multiStream = singleInputStream.transform(boltId, outType, boltWrapperMultipleOutputs);
} else {
    final MergedInputsBoltWrapper<Tuple, SplitStreamType<Tuple>> boltWrapperMultipleOutputs = new MergedInputsBoltWrapper<Tuple, SplitStreamType<Tuple>>(bolt, boltId, null);
    boltWrapperMultipleOutputs.setStormTopology(stormTopology);
    multiStream = mergedInputStream.transform(boltId, outType, boltWrapperMultipleOutputs);
}

final SplitStream<SplitStreamType<Tuple>> splitStream = multiStream.split(new StormStreamSelector<Tuple>());

final HashMap<String, DataStream<Tuple>> op = new HashMap<>();
for (String outputStreamId : boltOutputs.keySet()) {
    op.put(outputStreamId,splitStream.select(outputStreamId).map(new SplitStreamMapper<Tuple>()));
    SingleOutputStreamOperator<Tuple> outStream = splitStream.select(outputStreamId).map(new SplitStreamMapper<Tuple>());
    outStream.getTransformation().setOutputType(declarer.getOutputType(outputStreamId));
    op.put(outputStreamId, outStream);
}
availableInputs.put(boltId, op);
outputStream = multiStream;
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`getOutputType()`方法主要获得当前bolt的转为Flink Operator算子后的数据类型。观察该方法的实现，整体逻辑比较简单。首先，获得当前bolt输出字段的个数n并进行校验。然后，构建Tuple<n + 1>对象，字段类型为DefaultComparable。读者可能很好奇为什么要在原有字段个数的基础上加上1，后文会对此做出解释。
```
TypeInformation<Tuple> getOutputType(final String streamId) throws IllegalArgumentException {
    if (streamId == null) {
        return null;
    }
    
    Fields outputSchema = this.outputStreams.get(streamId);
    if (outputSchema == null) {
        throw new IllegalArgumentException("Stream with ID '" + streamId
            + "' was not declared.");
    }

    Tuple t;
    final int numberOfAttributes = outputSchema.size();

    if (numberOfAttributes <= 24) {
        try {
            t = Tuple.getTupleClass(numberOfAttributes + 1).newInstance(); 
        } catch (final InstantiationException e) {
            throw new RuntimeException(e);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    } else {
        throw new IllegalArgumentException("Flink supports only a maximum number of 24 attributes");
    }

    for (int i = 0; i < numberOfAttributes + 1; ++i) {
        t.setField(new DefaultComparable(), i);
    }

    return TypeExtractor.getForObject(t);
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;接下来，我们看下BoltWrapper类的框架结构。BoltWrapper类继承了AbstractStreamOperator类并实现了OneInputStreamOperator，提供了一个成员变量IRichBolt bolt以作为IRichBolt的包装类。在BoltWrapper的构造方法中，完成基本的变量赋值，将该bolt的输入流字段信息保存到inputSchemas变量中，并调用`WrapperSetupHelper.getNumberOfAttributes(bolt, rawOutputs)`方法获得当前bolt对应的输出字段的个数。另外，也重写了`open()`、`processElement()`方法，这个方法我们在后文介绍。
```
public class BoltWrapper<IN, OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN, OUT> {
    protected final IRichBolt bolt;
    private final HashMap<Integer, Fields> inputSchemas = new HashMap<Integer, Fields>();
    ...
    public BoltWrapper(final IRichBolt bolt, final String name, final String inputStreamId,final String inputComponentId, final Fields inputSchema,final Collection<String> rawOutputs) throws IllegalArgumentException {
        this.bolt = bolt;
        this.name = name;
        this.inputSchemas.put(null, inputSchema);
        this.numberOfAttributes = WrapperSetupHelper.getNumberOfAttributes(bolt, rawOutputs);
    }
    ...
    @Override
    public void open() throws Exception {
        ...
    }
    ...
    @Override
    public void processElement(final StreamRecord<IN> element) throws Exception {
        ...
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MergedInputsBoltWrapper继承于BoltWrapper，主要用于对具有多个输入流的bolt包装。其构造方法继承于BoltWrapper的实现，`processElement()`方法在后文中说明。
```
public final class MergedInputsBoltWrapper<IN, OUT> extends BoltWrapper<StormTuple<IN>, OUT> {
    ...
    public MergedInputsBoltWrapper(final IRichBolt bolt, final String name, final Collection<String> rawOutputs)throws IllegalArgumentException {
        super(bolt, name, null, null, null, rawOutputs);
    }

    @Override
    public void processElement(final StreamRecord<StormTuple<IN>> element) throws Exception {
        ...
    }
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;至此，对bolts进行转换的工作已经完成。结合整个流程，总结为以下几个步骤：
* 使用双层while循环遍历bolts中的元素。
* 当前availableInputs变量是否包含当前bolt所需要的输入流信息（Flink），如果不满足的话，跳出内层循环，继续遍历其他bolt。如果满足的话，继续向下执行。
* 为当前bolt的数据流信息（Flink）设置分区策略。
* 根据当前bolt具有输入流、输出流的个数，连接上游算子（Flink），创建该bolt对应的Flink Operator算子信息。
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;随着spouts、bolts转换工作的完成，Storm拓扑也完成了向Flink拓扑的转变。同样，也留下了几个问题：
* 为什么bolt在转换为Flink Operator算子后的数据流输出字段个数会比原有的字段个数多1？
* 转换后的Flink拓扑是如何完成之前的Storm程序工作？
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;带着上述两个问题，我们对SpoutWrapper、BoltWrapper两个类进行深入分析。
### Spout与Bolt在Flink中的执行
#### SpoutWrapper类的源码分析
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SpoutWrapper继承RichParallelSourceFunction类，自定义了`run()`方法。Flink任务执行的时候，执行`SourceFunction#run()`方法以读取数据。在`SpoutWrapper#run()`方法中，首先会配置StormConfig对象，然后创建FlinkTopologyContext对象stormTopologyContext、SpoutCollector对象collector等，最后就是调用用户自定义的`open()`	,`run()`等方法。这样，Storm程序Spout部分所调用的方法也能够在Flink环境中调用，实现了转换的目的。
```
public final class SpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> {
    ...
    @Override
    public final void run(final SourceContext<OUT> ctx) throws Exception {
        final GlobalJobParameters config = super.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        StormConfig stormConfig = new StormConfig();

        if (config != null) {
            if (config instanceof StormConfig) {
                stormConfig = (StormConfig) config;
            } else {
                stormConfig.putAll(config.toMap());
            }
        }

        final TopologyContext stormTopologyContext = WrapperSetupHelper.createTopologyContext(
                    (StreamingRuntimeContext) super.getRuntimeContext(), this.spout, this.name,this.stormTopology, stormConfig);

        SpoutCollector<OUT> collector = new SpoutCollector<OUT>(this.numberOfAttributes,stormTopologyContext.getThisTaskId(), ctx);

        this.spout.open(stormConfig, stormTopologyContext, new SpoutOutputCollector(collector));
        this.spout.activate();
        ...
        this.spout.nextTuple();
        ...
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;FlinkTopologyContext类继承TopologyContext类，其构造方法实际上是构建了TopologyContext实例。只不过重写了在Flink中执行Storm拓扑时不适用的某些方法，并抛出异常。对于每个并行子任务，都会存在一个FlinkTopologyContext对象。
```
final class FlinkTopologyContext extends TopologyContext {

    FlinkTopologyContext(...) {
        super(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId,
                codeDir, pidDir, taskId, workerPort, workerTasks, defaultResources, userResources, executorData,
                    registeredMetrics, openOrPrepareWasCalled);
    }

    ...
    @Override
    public void addTaskHook(final ITaskHook hook) {
        throw new UnsupportedOperationException("Task hooks are not supported by Flink");
    }
    ...
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SpoutCollector类继承AbstractStormCollector类并实现了ISpoutOutputCollector接口，其将Storm发送出去的Tuple元组转换为Flink Tuple元组，并通过提供的SourceContext对象flinkContext将数据发送出去。
```
class SpoutCollector<OUT> extends AbstractStormCollector<OUT> implements ISpoutOutputCollector {

    private final SourceContext<OUT> flinkContext;

    SpoutCollector(final HashMap<String, Integer> numberOfAttributes, final int taskId,final SourceContext<OUT> flinkContext) 
                        throws UnsupportedOperationException {
        super(numberOfAttributes, taskId);
        assert (flinkContext != null);
        this.flinkContext = flinkContext;
    }

    @Override
    protected List<Integer> doEmit(final OUT flinkTuple) {
        this.flinkContext.collect(flinkTuple);
        return null;
    }
    ...
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在`SpoutWrapper#run()`方法中，通过`WrapperSetupHelper.createTopologyContext()`方法对象TopologyContext对象。接下来，我们详细看下这个方法的内部实现。`context.getNumberOfParallelSubtasks()`方法获得当前source、operator算子的并行度，并初始化taskToComponents、componentToSortedTasks、componentToStreamToFields变量。taskToComponents变量，用于保存Flink拓扑taskId与节点名称的映射关系，数据类型为`Map<Integer, String>`。componentToSortedTasks变量，用于保存当前节点与启动的taskId之间的映射关系，数据类型为`Map<String, List<Integer>>`。componentToStreamToFields变量，用于保存当前节点与其输出输出字段之间的映射关系，数据类型为`Map<String, Map<String, Fields>>`。接下来使用三个for代码块，用于确定当前source/operator算子的taskId。第一个for循环，确定spouts中的spout在上述三个变量的赋值情况。第二个for循环用于确定bolts中的每一个bolt在上述变量的赋值情况。第三个同理，但是很少用到，就不做介绍。`processSingleOperator()`用于确定当前算子在Flink拓扑中的taskId，如果当前遍历的Storm节点信息（spout、bolt）与当前Flink拓扑执行的算子Opertor是同一个的话，返回该方法计算得到taskId，否则返回null，继续执行遍历，直到找到taskId。Storm提供了tick-tuple定时机制，Flink-Storm工具也配置默认定时时长，但是该部分功能仍然需要读者在SpoutWrapper/BoltWrapper类中进行完善。最后，根据获得的taskToComponents，componentToSortedTasks，componentToStreamToFields等参数初始化FlinkTopologyContext对象。
```
static synchronized TopologyContext createTopologyContext(final StreamingRuntimeContext context, 
        final IComponent spoutOrBolt,final String operatorName, StormTopology stormTopology, final Map stormConfig) {

    final int dop = context.getNumberOfParallelSubtasks(); 

    final Map<Integer, String> taskToComponents = new HashMap<Integer, String>();
    final Map<String, List<Integer>> componentToSortedTasks = new HashMap<String, List<Integer>>();
    final Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();
    ...
    // whole topology is built (i.e. FlinkTopology is used)
    Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
    Map<String, Bolt> bolts = stormTopology.get_bolts();
    Map<String, StateSpoutSpec> stateSpouts = stormTopology.get_state_spouts();

    tid = 1;

    for (Entry<String, SpoutSpec> spout : spouts.entrySet()) {
        Integer rc = processSingleOperator(spout.getKey(), spout.getValue().get_common(),operatorName, 
                        context.getIndexOfThisSubtask(), dop, taskToComponents,componentToSortedTasks,
                            componentToStreamToFields);
        if (rc != null) {
            taskId = rc;
        }
    }

    for (Entry<String, Bolt> bolt : bolts.entrySet()) {
        Integer rc = processSingleOperator(bolt.getKey(), bolt.getValue().get_common(),operatorName, 
                        context.getIndexOfThisSubtask(), dop, taskToComponents,componentToSortedTasks, 
                            componentToStreamToFields);
        if (rc != null) {
            taskId = rc;
        }
    }

    for (Entry<String, StateSpoutSpec> stateSpout : stateSpouts.entrySet()) {
        Integer rc = processSingleOperator(stateSpout.getKey(), stateSpout.getValue().get_common(), operatorName, 
                        context.getIndexOfThisSubtask(),dop, taskToComponents, componentToSortedTasks, 
                            componentToStreamToFields);
        if (rc != null) {
            taskId = rc;
        }
    }

    ...

    if (!stormConfig.containsKey(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
        stormConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30); // Storm default value
    }

    return new FlinkTopologyContext(stormTopology, stormConfig, taskToComponents,
        componentToSortedTasks, componentToStreamToFields, stormId, codeDir, pidDir,
        taskId, workerPort, workerTasks, defaultResources, userResources, executorData,
        registeredMetrics, openOrPrepareWasCalled);
}
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`processSingleOperator()`方法用于确定当前taskToComponents、componentToSortedTasks、componentToStreamToFields变量值，并提供当前节点唯一的taskid。首先，获得当前Storm节点（spout、bolt）并行度数目。如果当前Storm节点（spout、bolt）是当前Flink执行的算子，根据`context.getIndexOfThisSubtask()`得到的当前算子的subtask下标以及当前tid数就是当前算子的taskId，否则为null。继续向下执行，主要将当前遍历的Storm节点（spout、bolt）对应的taskId以及输出字段等信息保存到变量中。
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;读者千万不能认为靠前的算子其taskId就会比靠后的taskId要小，因为记录在StormTopology对象的bolts变量中的元素本来就是乱序的，所以其taskId也不会按照先后顺序进行排序，但是同一个算子的taskId其是连续递增的。在taskToComponents、componentToSortedTasks、componentToStreamToFields变量在当前Storm WordCount程序的取值如下所示：
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;taskToComponents：{1=source, 2=source, 3=counter, 4=counter, 5=sink, 6=sink, 7=tokenizer, 8=tokenizer}
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;componentToSortedTasks：{sink=[5, 6], source=[1, 2], counter=[3, 4], tokenizer=[7, 8]}
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;componentToStreamToFields：{sink={}, source={default=[sentence]}, counter={default=[word, count]}, tokenizer={default=[word, count]}}
```
private static Integer processSingleOperator(final String componentId, final ComponentCommon common, 
        final String operatorName, final int index,final int dop, 
        final Map<Integer, String> taskToComponents,
        final Map<String, List<Integer>> componentToSortedTasks,
        final Map<String, Map<String, Fields>> componentToStreamToFields) {
        
    final int parallelismHint = common.get_parallelism_hint();
    Integer taskId = null;

    if (componentId.equals(operatorName)) {
        taskId = tid + index;
    }

    List<Integer> sortedTasks = new ArrayList<Integer>(dop);
    for (int i = 0; i < parallelismHint; ++i) {
        taskToComponents.put(tid, componentId);
        sortedTasks.add(tid);
        ++tid;
    }
    componentToSortedTasks.put(componentId, sortedTasks);

    Map<String, Fields> outputStreams = new HashMap<String, Fields>();
    for (Entry<String, StreamInfo> outStream : common.get_streams().entrySet()) {
        outputStreams.put(outStream.getKey(), new Fields(outStream.getValue().get_output_fields()));
    }
    componentToStreamToFields.put(componentId, outputStreams);

    return taskId;
}
```
#### BoltWrapper类的源码分析
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;BoltWrapper继承AbstractStreamOperator类并实现了OneInputStreamOperator接口，重写了`open()`、`processElement()`方法。在Flink执行Operator算子的时候，首先会执行其`open()`方法，然后在执行`processElement()`方法。在`open()`方法中，首先会配置StormConfig对象，创建FlinkTopologyContext对象，初始化BoltWrapper对象，并构建OutputCollector对象。紧接着，从FlinkTopologyContext对象topologyContext中获得当前节点（bolt）的输入流信息inputs。遍历inputs，将该节点（bolt）的输入流的taskId以及对应的componentId、streamId、输出字段等信息保存到inputComponentIds、inputStreamIds、inputSchemas等变量中，供`processElement()`方法使用，紧接着会调用用户自定义bolt的`prepare()`方法。在`processElement()`方法中，首先获得数据流元素value，Flink Tuple类型，并且Tuple最后一个位置记录着当前数据从哪个taskId中发送，也可以获得当前数据从哪个componentId、streamId发送而来以及其对应的输出数据格式是什么，这也是为什么每个Storm节点（spout、bolt）的输出字段个数在转换后会多一个的原因。用户的自定义的bolt中也可以通过这些参数完成不同的业务需求。最后，会将value以及上述参数封装StormTuple实例，供用户自定义的bolt执行。
```
public class BoltWrapper<IN, OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN, OUT> {
    ...
    @Override
    public void open() throws Exception {
        super.open();

        this.flinkCollector = new TimestampedCollector<>(this.output);

        GlobalJobParameters config = getExecutionConfig().getGlobalJobParameters();
        StormConfig stormConfig = new StormConfig();

        if (config != null) {
            if (config instanceof StormConfig) {
                stormConfig = (StormConfig) config;
            } else {
                stormConfig.putAll(config.toMap());
            }
        }

        this.topologyContext = WrapperSetupHelper.createTopologyContext(getRuntimeContext(),
                                        this.bolt, this.name, this.stormTopology, stormConfig);

        final OutputCollector stormCollector = new OutputCollector(new BoltCollector<OUT>(
                            this.numberOfAttributes, this.topologyContext.getThisTaskId(), this.flinkCollector));

        if (this.stormTopology != null) {
            Map<GlobalStreamId, Grouping> inputs = this.topologyContext.getThisSources();

            for (GlobalStreamId inputStream : inputs.keySet()) {
                for (Integer tid : this.topologyContext.getComponentTasks(inputStream.get_componentId())) {
                    this.inputComponentIds.put(tid, inputStream.get_componentId());
                    this.inputStreamIds.put(tid, inputStream.get_streamId());
                    this.inputSchemas.put(tid,this.topologyContext.getComponentOutputFields(inputStream));
                }
            }
        }

        this.bolt.prepare(stormConfig, this.topologyContext, stormCollector);
    }

    ...

    @Override
    public void processElement(final StreamRecord<IN> element) throws Exception {
        this.flinkCollector.setTimestamp(element);

        IN value = element.getValue();

        if (this.stormTopology != null) {
            Tuple tuple = (Tuple) value;
            Integer producerTaskId = tuple.getField(tuple.getArity() - 1);

            this.bolt.execute(new StormTuple<>(value, this.inputSchemas.get(producerTaskId),                                
                                    producerTaskId, this.inputStreamIds.get(producerTaskId), 
                                    this.inputComponentIds.get(producerTaskId), MessageId.makeUnanchored()));

        } else {
            this.bolt.execute(new StormTuple<>(value, this.inputSchemas.get(null), -1, null, null,MessageId.makeUnanchored()));
        }
    }
}
```
## 总结
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flink-Storm工具Flink官方提供的一个迁移Storm任务的beta工具，虽然有一些场景并不能很好的支持，如用户Storm代码中启动多个线程，但是在一些简单任务场景中能够实现一键迁移的功能，大大降低了开发成本。Flink-Storm工具在提供给我们迁移Storm任务便利的同时，也为我们提供了两种不同实时计算引擎的思想。