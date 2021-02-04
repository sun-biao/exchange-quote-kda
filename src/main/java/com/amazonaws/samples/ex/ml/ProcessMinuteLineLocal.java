/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.samples.ex.ml;

import com.amazonaws.samples.ex.ml.events.TradeData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import com.amazonaws.regions.Regions;
import com.amazonaws.samples.kaja.taxi.consumer.events.EventDeserializationSchema;
import com.amazonaws.samples.kaja.taxi.consumer.events.kinesis.Event;
import com.amazonaws.samples.kaja.taxi.consumer.events.kinesis.TripEvent;
import com.amazonaws.samples.kaja.taxi.consumer.utils.GeoUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;


public class ProcessMinuteLineLocal {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessMinuteLineLocal.class);

  private static final String DEFAULT_STREAM_NAME = "streaming-analytics-workshop";
  private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion()==null ? "ap-northeast-1" : Regions.getCurrentRegion().getName();
  private static final String s3SinkPath = "s3a://tokyo-sunbiao-firehose/data";
  private static final ObjectMapper jsonParser = new ObjectMapper();
  private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {

    Properties inputProperties = new Properties();
    inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, DEFAULT_REGION_NAME);
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
            "LATEST");
    return env.addSource(new FlinkKinesisConsumer<>(DEFAULT_STREAM_NAME,
            new SimpleStringSchema(),
            inputProperties));
  }



  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(50);//默认100毫秒



    //read the parameters specified from the command line
    ParameterTool parameter = ParameterTool.fromArgs(args);


    Properties kinesisConsumerConfig = new Properties();
    //set the region the Kinesis stream is located in
    kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, parameter.get("Region", DEFAULT_REGION_NAME));
    //obtain credentials through the DefaultCredentialsProviderChain, which includes credentials from the instance metadata
    kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
    //poll new events from the Kinesis stream once every second
    kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");


    //create Kinesis source
    DataStream<String> kinesisStream = createSourceFromStaticConfig(env);
    DataStream<TradeData> basicstream =kinesisStream.map(value -> {
      JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
      System.out.println("----------------------------"+ value + "----------------------------------");
      return new TradeData(jsonNode.get("TICKER").asText(),
              jsonNode.get("PRICE").asDouble(),jsonNode.get("EventTime").asLong());
    });


    basicstream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner(new SerializableTimestampAssigner<TradeData>() {
                      @Override
                      public long extractTimestamp(TradeData element, long recordTimestamp) {
                        return element.getEventtime(); //指定EventTime对应的字段
                      }
                    }))
            .keyBy(new KeySelector<TradeData, String>(){
      @Override
      public String getKey(TradeData value) throws Exception {
        return value.getTicker();  //按照bidui分组
      }}).timeWindow(Time.seconds(5)).trigger(new CustomProcessingTimeTrigger()).aggregate(new MLAggregate("5 sec"), new MyProcessWindowFunction()).print();

    basicstream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner<TradeData>() {
              @Override
              public long extractTimestamp(TradeData element, long recordTimestamp) {
                return element.getEventtime(); //指定EventTime对应的字段
              }
            }))
            .keyBy(new KeySelector<TradeData, String>(){
              @Override
              public String getKey(TradeData value) throws Exception {
                return value.getTicker();  //按照bidui分组
              }}).timeWindow(Time.seconds(10)).trigger(new CustomProcessingTimeTrigger()).aggregate(new MLAggregate("10sec"), new MyProcessWindowFunction()).print();

//    DataStream<TripEvent> trips = kinesisStream
//        //remove all events that aren't TripEvents
//        .filter(event -> TripEvent.class.isAssignableFrom(event.getClass()))
//        //cast Event to TripEvent
//        .map(event -> (TripEvent) event)
//        //remove all events with geo coordinates outside of NYC
//        .filter(GeoUtils::hasValidCoordinates);


    //print trip events to stdout
    //kinesisStream.print();


    LOG.info("Reading events from stream {}", parameter.get("InputStreamName", DEFAULT_STREAM_NAME));

    env.execute();
  }



  private static class MLAggregate
          implements AggregateFunction<TradeData, Tuple6<String, Long, Double, Double, Double,Double>, Tuple7<String, Long, Double, Double, Double,Double,String>> {

    private String mltype;
    @Override
    public Tuple6<String, Long, Double, Double, Double,Double> createAccumulator() {
      return new Tuple6<String, Long, Double, Double, Double,Double>("", 999999999999999999L, 99999999999999999D, 0D, 0D, 0D);//pair name, time, min price, max price, earliest price, latest price

    }

    public MLAggregate(String mltype){
      super();
      this.mltype = mltype;
    }

    @Override
    public Tuple6<String, Long, Double, Double, Double,Double> add(TradeData value, Tuple6<String, Long, Double, Double, Double,Double> accumulator) {
      accumulator.f0 = value.getTicker();
      accumulator.f4 = value.getEventtime() < accumulator.f1 ? value.getPrice() : accumulator.f4;
      accumulator.f5 =  value.getPrice(); // we assume it's ordered
      accumulator.f1 = Math.min(value.getEventtime(),accumulator.f1 );
      accumulator.f2 = Math.min(accumulator.f2,value.getPrice() );
      accumulator.f3 = Math.max(accumulator.f3,value.getPrice() );
      return new Tuple6<String, Long, Double, Double, Double,Double>(accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5);
    }

    @Override
    public  Tuple7<String, Long, Double, Double, Double,Double,String> getResult(Tuple6<String, Long, Double, Double, Double,Double> accumulator) {
      return new Tuple7<>(accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3,accumulator.f4,accumulator.f5,this.mltype);
    }

    @Override
    public Tuple6<String, Long, Double, Double, Double,Double>  merge(Tuple6<String, Long, Double, Double, Double,Double> accumulator1, Tuple6<String, Long, Double, Double, Double,Double> accumulator2) {
      return new Tuple6<String, Long, Double, Double, Double,Double>(accumulator1.f0,accumulator1.f1,Math.min(accumulator1.f2,accumulator2.f2),Math.max(accumulator1.f3,accumulator2.f3),accumulator1.f4,accumulator2.f5);
    }
  }

  private static class MyProcessWindowFunction
          extends ProcessWindowFunction<Tuple7<String, Long, Double, Double, Double, Double,String>, String, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple7<String, Long, Double, Double, Double, Double,String>> iterable, Collector<String> collector) throws Exception {
      Tuple7<String, Long, Double, Double, Double, Double, String> curr = iterable.iterator().next();
      collector.collect(curr.f6+":  "+curr.f0 + "  "  + curr.f1 + "  " + curr.f2 + "   " + curr.f3 + "    " + curr.f4 + "   " + curr.f5 + "    ");
    }
  }

  private static class CustomProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
      System.out.println("element fire");
      return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
      System.out.println("processing contiue");
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
      //System.out.println("event contiue");
      return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
      //System.out.println("clear");
    }
  }

}
