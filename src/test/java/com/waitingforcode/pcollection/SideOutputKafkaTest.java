package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Date;

@RunWith(JUnit4.class)
public class SideOutputKafkaTest implements Serializable {

    @Test
    public void should_show_that_side_output_is_processed_in_streaming_for_unbounded_source() {
        Pipeline pipeline = BeamFunctions.createPipeline("Kafka reader with side outputs");

        PCollection<KafkaRecord<Long, String>> kafkaRecords = pipeline.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("beam_test")
                .withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
        );
        TupleTag<String> stringifiedBooleans = new TupleTag<String>(){};
        TupleTag<String> integerBooleans = new TupleTag<String>(){};

        Duration windowDuration = Duration.standardMinutes(2);
        Window<KafkaRecord<Long, String>> window = Window.into(FixedWindows.of(windowDuration));

        PCollectionTuple dataset = kafkaRecords.apply(window).apply(ParDo.of(new DoFn<KafkaRecord<Long, String>, String>() {

            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KafkaRecord<Long, String> element = processContext.element();
                System.out.println("Reading record " + element.getKV() + " at " + new Date());
                long timestamp = System.currentTimeMillis();
                processContext.output(timestamp + " - " + element.getKV().getValue());
                processContext.output(integerBooleans, timestamp + " - " + element.getKV().getValue());
            }

        }).withOutputTags(stringifiedBooleans, TupleTagList.of(integerBooleans)));

        // #1 side output
        dataset.get(stringifiedBooleans).apply(ParDo.of(new DoFn<String, String>() {
            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println("[1] Processing " + processContext.element() + " at " + new Date());
            }
        }));
        // #2 side output
        dataset.get(integerBooleans).apply(ParDo.of(new DoFn<String, String>() {
            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println("[2] Processing " + processContext.element() + " at " + new Date());
            }
        }));
        pipeline.run().waitUntilFinish();
    }

}
