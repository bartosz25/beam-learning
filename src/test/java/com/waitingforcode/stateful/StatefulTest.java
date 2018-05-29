package com.waitingforcode.stateful;


import avro.shaded.com.google.common.base.Joiner;
import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

@RunWith(JUnit4.class)
public class StatefulTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private static final Instant SEC_2_DURATION = NOW.plus(Duration.standardSeconds(2));
    private static final Instant SEC_5_DURATION = NOW.plus(Duration.standardSeconds(5));
    private static final Instant SEC_6_DURATION = NOW.plus(Duration.standardSeconds(6));

    @Test
    public void should_fail_on_applying_stateful_transform_for_not_key_value_pairs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Shared state example");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> words = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("cat", SEC_1_DURATION), TimestampedValue.of("hat", SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration));

        assertThatThrownBy(() -> {
            pipeline.apply(words).apply(window).apply(ParDo.of(new MinLengthTextSharedStateProcessing(2)));
            pipeline.run().waitUntilFinish();
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ParDo requires its input to use KvCoder in order to use state and timers.");
    }

    @Test
    public void should_correctly_increment_counter_for_each_encountered_item() {
        Pipeline pipeline = BeamFunctions.createPipeline("Counter example");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Integer> varIntCoder = VarIntCoder.of();
        KvCoder<String, Integer> keyValueCoder = KvCoder.of(utf8Coder, varIntCoder);
        TestStream<KV<String, Integer>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 2), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b", 5), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 6), SEC_1_DURATION),
                TimestampedValue.of(KV.of("c", 2), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 7), SEC_1_DURATION),
                TimestampedValue.of(KV.of("a", 3), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 9), SEC_1_DURATION),
                TimestampedValue.of(KV.of("d", 2), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 1), SEC_1_DURATION),
                TimestampedValue.of(KV.of("a", 2), SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<KV<String, Integer>> window = Window.<KV<String, Integer>>into(FixedWindows.of(windowDuration))
                .triggering(AfterPane.elementCountAtLeast(20))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = pipeline.apply(words).apply(window)
                .apply(ParDo.of(new DoFn<KV<String, Integer>, String>() {
            private static final String COUNTER_NAME = "counter";

            // Its definition is required, otherwise the following exception is thrown:
            // parameter of type MapState<String, Integer> at index 1: reference to undeclared StateId: "counter"
            @StateId(COUNTER_NAME)
            private final StateSpec<MapState<String, Integer>> mapState = StateSpecs.map();

            @ProcessElement
            public void process(ProcessContext processContext,
                    @StateId(COUNTER_NAME) MapState<String, Integer> letterCounterState) {
                KV<String, Integer> element = processContext.element();
                ReadableState<Integer> letterSumState = letterCounterState.get(element.getKey());
                int currentSum = letterSumState.read() != null ? letterSumState.read() : 0;
                int letterSum = currentSum + element.getValue();
                letterCounterState.put(element.getKey(), letterSum);
                processContext.output(element.getKey()+"="+letterSum);
            }

        }));
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // It also shows that state is not adapted to be returned every time because instead of having a, b,c and d
        // with final values, we have intermediary results
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=31", "a=29", "a=28", "a=19", "a=16", "a=9",
                "a=3", "b=5", "a=1", "c=2", "d=2");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_show_the_use_of_combine_state() {
        Pipeline pipeline = BeamFunctions.createPipeline("Combined state example");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);
        TestStream<KV<String, String>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("10:00", "/index.html"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("10:01", "/cart.html"), SEC_2_DURATION),
                TimestampedValue.of(KV.of("10:01", "/cancel_order.html"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("10:05", "/delivery.html"), SEC_5_DURATION),
                TimestampedValue.of(KV.of("10:02", "/index.html"), SEC_2_DURATION),
                TimestampedValue.of(KV.of("10:06", "/cart.html"), SEC_6_DURATION),
                TimestampedValue.of(KV.of("10:01", "/login.html"), SEC_6_DURATION),
                TimestampedValue.of(KV.of("10:07", "/payment.html"), SEC_6_DURATION.plus(Duration.standardSeconds(1))),
                TimestampedValue.of(KV.of("10:08", "/order_confirmation.html"), SEC_6_DURATION.plus(Duration.standardSeconds(2)))
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<KV<String, String>> window = Window.<KV<String, String>>into(FixedWindows.of(windowDuration))
                .triggering(AfterPane.elementCountAtLeast(20))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<KV<String, Integer>> results = pipeline.apply(words).apply(window).apply(ParDo.of(new SessionTransform()));
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // This assert shows that the stateful processing doesn't work well when we output the computed values every time
        // The output for 10:01 is returned twice
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder(KV.of("10:00", 1), KV.of("10:01", 2), KV.of("10:02", 1),
                KV.of("10:01", 1));
        // The presence of 10:01 with different value than in previous window proves that the state is bounded
        // to the window duration
        IntervalWindow window2 = new IntervalWindow(NOW.plus(windowDuration), NOW.plus(windowDuration).plus(windowDuration));
        PAssert.that(results).inFinalPane(window2).containsInAnyOrder(KV.of("10:05", 1), KV.of("10:06", 1),
                KV.of("10:07", 1), KV.of("10:08", 1), KV.of("10:01", 1));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_accumulate_encountered_words_with_bagstate() {
        Pipeline pipeline = BeamFunctions.createPipeline("Bag state example");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);
        TestStream<KV<String, String>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("p", "paradigm"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("p", "programming"), SEC_1_DURATION)
        )
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<KV<String, String>> window = Window.<KV<String, String>>into(FixedWindows.of(windowDuration))
                .triggering(AfterPane.elementCountAtLeast(20))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = pipeline.apply(words).apply(window)
                .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
            private static final String ACCUMULATOR_NAME = "accumulator";

            @StateId(ACCUMULATOR_NAME)
            private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

            @ProcessElement
            public void processElement(ProcessContext processContext,
                    @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator) {
                KV<String, String> letterWordPair = processContext.element();
                wordsAccumulator.add(letterWordPair.getValue());
                processContext.output(Joiner.on(",").join(wordsAccumulator.read()));
            }
        }));

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // Since the ordering is not deterministic, we check against 2 possible outputs
        PAssert.that(results).inFinalPane(window1).satisfies((SerializableFunction<Iterable<String>, Void>) input -> {
            List<String> expectedWords = Arrays.asList("paradigm", "paradigm,programming",
                    "programming", "programming,paradigm");
            int matchedWords = 0;
            for (String generatedWord : input) {
                if (expectedWords.contains(generatedWord)) {
                    matchedWords++;
                }
            }
            Assert.assertEquals(matchedWords, 2);
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_show_that_stateful_processing_order_is_also_not_deterministic() {
        List<KV<String, String>> clubsPerCountry = Arrays.asList(
                KV.of("Germany", "VfB Stuttgart"), KV.of("Germany", "Bayern Munich"), KV.of("Germany", "FC Koln"));

        for (int i = 0; i < 10; i++) {
            Pipeline pipeline = BeamFunctions.createPipeline("Not deterministic stateful processing");
            pipeline.apply(Create.of(clubsPerCountry))
                    .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
                        private static final String ACCUMULATOR_NAME = "accumulator";

                        @StateId(ACCUMULATOR_NAME)
                        private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

                        @ProcessElement
                        public void processElement(ProcessContext processContext,
                                                   @StateId(ACCUMULATOR_NAME) BagState<String> clubsAccumulator) {
                            clubsAccumulator.add(processContext.element().getValue());
                            String clubs = Joiner.on("-").join(clubsAccumulator.read());
                            StringsAccumulator.CLUBS.add(clubs);
                            processContext.output(clubs);
                        }
                    }));
            pipeline.run().waitUntilFinish();
        }
        // We deal with 3 keys. If the processing order would be guaranteed, we'd always retrieve the same pairs.
        // It means we'd have the size of accumulated entries equal to 3. But since the processing order is not
        // deterministic, obviously we have more items than that.
        assertThat(StringsAccumulator.CLUBS.getEntries().size()).isGreaterThan(3);
    }

    @Test
    public void should_show_that_stateful_processing_is_key_based() {
        List<KV<String, String>> clubsPerCountry = Arrays.asList(
                KV.of("Germany", "VfB Stuttgart"), KV.of("Germany", "Bayern Munich"), KV.of("Germany", "FC Koln"),
                KV.of("Holland", "Ajax Amsterdam"), KV.of("Holland", "Sparta Rotterdam"),
                KV.of("Spain", "FC Barcelona"), KV.of("Spain", "Real Madrid"));

        Pipeline pipeline = BeamFunctions.createPipeline("Not deterministic stateful processing");
        PCollection<String> results = pipeline.apply(Create.of(clubsPerCountry))
                .apply(ParDo.of(new DoFn<KV<String, String>, String>() {

                    private static final String COUNTER_NAME = "occurrences_counter";

                    @StateId(COUNTER_NAME)
                    private final StateSpec<ValueState<Integer>> counter = StateSpecs.value(VarIntCoder.of());

                    @ProcessElement
                    public void processElement(ProcessContext processContext,
                                               @StateId(COUNTER_NAME) ValueState<Integer> counterState) {
                        int currentValue = Optional.ofNullable(counterState.read()).orElse(0);
                        int incrementedCounter = currentValue + 1;
                        counterState.write(incrementedCounter);
                        processContext.output(processContext.element().getKey()+"="+incrementedCounter);
                    }
                }));

        // If the state cell wasn't be key-based, the number of accumulated entries would probably be incremental,
        // i.e. 1, 2, 3, 4, 5, 6, 7
        PAssert.that(results).containsInAnyOrder("Germany=1", "Germany=2", "Germany=3", "Holland=1", "Holland=2",
                "Spain=1", "Spain=2");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_fail_when_two_states_with_the_same_ids_are_defined() {
        Pipeline pipeline = BeamFunctions.createPipeline("Two states with the same name");
        List<KV<String, String>> clubsPerCountry = Arrays.asList(
                KV.of("Germany", "VfB Stuttgart"), KV.of("Germany", "Bayern Munich"), KV.of("Germany", "FC Koln"));

        assertThatThrownBy(() -> {
            pipeline.apply(Create.of(clubsPerCountry))
                    .apply(ParDo.of(new DoFn<KV<String, String>, String>() {

                        private static final String ACCUMULATOR_NAME = "accumulator";

                        @StateId(ACCUMULATOR_NAME)
                        private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

                        @StateId(ACCUMULATOR_NAME)
                        private final StateSpec<BagState<String>> accumulatorStateSpecDuplicated = StateSpecs.bag();

                        @ProcessElement
                        public void processElement(ProcessContext processContext,
                                                   @StateId(ACCUMULATOR_NAME) BagState<String> clubsAccumulator) {
                            clubsAccumulator.add(processContext.element().getValue());
                            String clubs = Joiner.on(", ").join(clubsAccumulator.read());
                            processContext.output(clubs);
                        }
                    }));


            pipeline.run().waitUntilFinish();
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate StateId \"accumulator\", used on both of " +
                        "[private final org.apache.beam.sdk.state.StateSpec ")
                .hasMessageContaining("and [private final org.apache.beam.sdk.state.StateSpec");
    }

    @Test
    public void should_fail_when_the_state_spec_declaration_is_not_final() {
        Pipeline pipeline = BeamFunctions.createPipeline("Two states with the same name");
        List<KV<String, String>> clubsPerCountry = Arrays.asList(
                KV.of("Germany", "VfB Stuttgart"), KV.of("Germany", "Bayern Munich"), KV.of("Germany", "FC Koln"));

        assertThatThrownBy(() -> {
            pipeline.apply(Create.of(clubsPerCountry))
                    .apply(ParDo.of(new DoFn<KV<String, String>, String>() {

                        private static final String ACCUMULATOR_NAME = "accumulator";

                        @StateId(ACCUMULATOR_NAME)
                        private StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

                        @ProcessElement
                        public void processElement(ProcessContext processContext,
                                                   @StateId(ACCUMULATOR_NAME) BagState<String> clubsAccumulator) {
                            clubsAccumulator.add(processContext.element().getValue());
                            String clubs = Joiner.on(", ").join(clubsAccumulator.read());
                            processContext.output(clubs);
                        }
                    }));


            pipeline.run().waitUntilFinish();
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Non-final field private").hasMessageContaining("org.apache.beam.sdk.state.StateSpec")
                .hasMessageContaining("annotated with StateId. State declarations must be final.");
    }

    enum StringsAccumulator {
        CLUBS;

        private Set<String> entries = new HashSet<>();

        public void add(String entry) {
            System.out.println("Adding " + entry);
            entries.add(entry);
        }

        public Set<String> getEntries() {
            return entries;
        }
    }


    public static class MinLengthTextSharedStateProcessing extends DoFn<String, String> {

        private static final String COUNTER_NAME = "counter";

        @StateId(COUNTER_NAME)
        private final StateSpec<ValueState<Integer>> counter = StateSpecs.value(VarIntCoder.of());

        private int textMinLength;

        public MinLengthTextSharedStateProcessing(int textMinLength) {
            this.textMinLength = textMinLength;
        }

        @ProcessElement
        public void process(ProcessContext processContext, @StateId(COUNTER_NAME) ValueState<Integer> counterState) {
            String word = processContext.element();
            if (word.length() >= textMinLength) {
                int counterValue = counterState.read();
                int newCounterValue = counterValue + 1;
                counterState.write(newCounterValue);
                processContext.output(word+"="+newCounterValue);
            }
        }

    }

    public static class SessionCombiner extends Combine.CombineFn<String, List<String>, Integer> {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<String> addInput(List<String> accumulator, String log) {
            accumulator.add(log);
            return accumulator;
        }

        @Override
        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
            List<String> mergedAccumulator = new ArrayList<>();
            accumulators.forEach(accumulatorToMerge -> mergedAccumulator.addAll(accumulatorToMerge));
            return mergedAccumulator;
        }

        @Override
        public Integer extractOutput(List<String> accumulator) {
            return accumulator.size();
        }
    }

    public static class SessionTransform extends DoFn<KV<String, String>, KV<String, Integer>> {

        private static final String COUNTER_NAME = "counter";

        @StateId(COUNTER_NAME)
        private final StateSpec<CombiningState<String, List<String>, Integer>> sessionStateSpec =
                StateSpecs.combining(ListCoder.of(StringUtf8Coder.of()), new SessionCombiner());

        @ProcessElement
        public void process(ProcessContext processContext,
                            @StateId(COUNTER_NAME) CombiningState<String, List<String>, Integer> sessionState) {
            KV<String, String> element = processContext.element();
            sessionState.add(element.getValue());
            processContext.output(KV.of(element.getKey(), sessionState.read()));
        }

    }

}