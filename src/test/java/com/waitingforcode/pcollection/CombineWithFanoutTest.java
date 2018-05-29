package com.waitingforcode.pcollection;

import avro.shaded.com.google.common.collect.Lists;
import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class CombineWithFanoutTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));

    @Test
    public void should_apply_combine_with_fanout() {
        Pipeline pipeline = BeamFunctions.createPipeline("Global combine with fanout", 2);
        TestStream<String> letters = TestStream.create(StringUtf8Coder.of()).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("b", SEC_1_DURATION),
                TimestampedValue.of("c", SEC_1_DURATION), TimestampedValue.of("d", SEC_1_DURATION),
                TimestampedValue.of("e", SEC_1_DURATION), TimestampedValue.of("f", SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<String> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<String> result = pipeline.apply(letters).apply(window)
                .apply(Combine.globally(new SerializableFunction<Iterable<String>, String>() {
            @Override
            public String apply(Iterable<String> input) {
                List<String> materializedInput = Lists.newArrayList(input);
                Collections.sort(materializedInput);
                String letters = String.join(",", materializedInput);
                FanoutResultHolder.INSTANCE.addPartialResults(letters);
                return letters;
            }
        }).withoutDefaults().withFanout(2));

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(result).inFinalPane(window1).containsInAnyOrder("a,c,e,b,d,f");
        pipeline.run().waitUntilFinish();
        assertThat(FanoutResultHolder.INSTANCE.getPartialResults()).containsOnly("a,c,e", "b,d,f", "a,c,e,b,d,f");
    }

    @Test
    public void should_apply_combine_with_hot_key_fanout() {
        Pipeline pipeline = BeamFunctions.createPipeline("Combine per key with fanout");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);
        TestStream<KV<String, String>> letters = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", "A"), SEC_1_DURATION), TimestampedValue.of(KV.of("a", "A"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b", "B"), SEC_1_DURATION), TimestampedValue.of(KV.of("c", "C"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("d", "D"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E1"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F1"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E2"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F2"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E3"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F3"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E4"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F4"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E5"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F5"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E6"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F6"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E7"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F7"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E8"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F8"), SEC_1_DURATION), TimestampedValue.of(KV.of("e", "E9"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("f", "F9"), SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<KV<String, String>> window = Window.into(FixedWindows.of(windowDuration));

        SerializableFunction<String, Integer> fanoutFunction = new SerializableFunction<String, Integer>() {
            @Override
            public Integer apply(String key) {
                // For the key f 2 intermediate nodes will be created
                // Since we've 9 values belonging to this key, one possible configuration
                // could be 5 and 4 entries per node
                return key.equals("f") ? 2 : 1;
            }
        };
        Combine.PerKeyWithHotKeyFanout<String, String, String> combineFunction =
                Combine.<String, String, String>perKey(new Combiner()).withHotKeyFanout(fanoutFunction);
        PCollection<KV<String, String>> lettersPCollection = pipeline.apply(letters).apply(window);
        PCollection<KV<String, String>> result = lettersPCollection.apply(combineFunction);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(result).inFinalPane(window1).containsInAnyOrder(KV.of("a", "A, A"), KV.of("b", "B"), KV.of("c", "C"),
                KV.of("d", "D"), KV.of("e", "E1, E2, E3, E4, E5, E6, E7, E8, E9"),
                KV.of("f", "F1, F2, F3, F4, F5, F6, F7, F8, F9"));
        pipeline.run().waitUntilFinish();
        assertThat(FanoutWithKeyResultHolder.INSTANCE.getValuesInCombiners()).containsOnly(
                "(empty)---(empty)---F2, F4, F6, F8", "(empty)---(empty)---F1, F3, F5, F7, F9",
                "(empty)---F1, F3, F5, F7, F9", "(empty)---F2, F4, F6, F8",
                "(empty)---(empty)---F1, F3, F5, F7, F9---F2, F4, F6, F8",
                "(empty)---(empty)---D", "(empty)---(empty)---B",
                "(empty)---(empty)---C", "(empty)---(empty)---A, A",
                "(empty)---(empty)---E1, E2, E3, E4, E5, E6, E7, E8, E9"
        );
    }


    public static class Combiner extends Combine.CombineFn<String, List<String>, String> {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<String> addInput(List<String> accumulator, String input) {
            accumulator.add(input);
            Collections.sort(accumulator);
            return accumulator;
        }

        @Override
        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
            List<String> mergedAccumulator = new ArrayList<>();
            accumulators.forEach(accumulatorToMerge -> mergedAccumulator.addAll(accumulatorToMerge));

            String valuesToMerge = Lists.newArrayList(accumulators).stream()
                    .flatMap(listOfLetters -> {
                        Collections.sort(listOfLetters);
                        if (listOfLetters.isEmpty()) {
                            return Stream.of("(empty)");
                        } else {
                            return Stream.of(String.join(", ", listOfLetters));
                        }
                    })
                    .sorted()
                    .collect(Collectors.joining("---"));
            FanoutWithKeyResultHolder.INSTANCE.addValues(valuesToMerge);
            return mergedAccumulator;
        }

        @Override
        public String extractOutput(List<String> accumulator) {
            Collections.sort(accumulator);
            return String.join(", ", accumulator);
        }
    }

    enum FanoutResultHolder {
        INSTANCE;

        private Set<String> partialResults = new HashSet<>();

        public Set<String> getPartialResults() {
            return partialResults;
        }

        public void addPartialResults(String partialResult) {
            partialResults.add(partialResult);
        }
    }

    enum FanoutWithKeyResultHolder {
        INSTANCE;

        private List<String> valuesInCombiners = new ArrayList<>();

        public void addValues(String values) {
            valuesInCombiners.add(values);
        }

        public List<String> getValuesInCombiners() {
            return valuesInCombiners;
        }
    }

}
