package com.waitingforcode.stateful;


import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnit4.class)
public class TimerTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private static final Instant SEC_3_DURATION = NOW.plus(Duration.standardSeconds(3));
    private static final Instant SEC_5_DURATION = NOW.plus(Duration.standardSeconds(5));
    private static final Instant SEC_12_DURATION = NOW.plus(Duration.standardSeconds(12));

    @Test
    public void should_save_accumulated_entries_after_window_expiration() {
        Pipeline pipeline = BeamFunctions.createPipeline("Timer expiration with window end");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 2L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b", 5L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 6L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("c", 2L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 7L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("a", 3L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 9L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("d", 2L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 1L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("a", 2L), SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<Long> results = pipeline.apply(words).apply(window).apply(ParDo.of(new DoFn<KV<String, Long>, Long>() {
            private static final String DATA_HOLDER_NAME = "sumState";
            private static final String EXPIRY_STATE_NAME = "expiry";

            @StateId(DATA_HOLDER_NAME)
            private final StateSpec<ValueState<Long>> sumStateSpec = StateSpecs.value(VarLongCoder.of());

            @TimerId(EXPIRY_STATE_NAME)
            private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext processContext, IntervalWindow window,
                                       @StateId(DATA_HOLDER_NAME) ValueState<Long> sumState,
                                       @TimerId(EXPIRY_STATE_NAME) Timer expiryTimer) {
                long currentState = Optional.ofNullable(sumState.read()).orElse(0L);
                if (sumState.read() == null) {
                    // timer will be triggered when the window ends
                    expiryTimer.set(window.maxTimestamp());
                }
                long newState = currentState + processContext.element().getValue();
                sumState.write(newState);
            }

            @OnTimer(EXPIRY_STATE_NAME)
            public void flushOnExpiringState(
                    OnTimerContext context,
                    @StateId(DATA_HOLDER_NAME) ValueState<Long> sumState) {
                context.output(sumState.read());
            }

        }));
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(results).inWindow(window1).containsInAnyOrder(31L, 5L, 2L, 2L);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_fail_on_expiring_timer_after_the_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Timer expiration after the end of the window");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 2L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b", 5L), SEC_1_DURATION), TimestampedValue.of(KV.of("a", 6L), SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(15);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        assertThatThrownBy(() -> {
            pipeline.apply(words).apply(window).apply(ParDo.of(new DoFn<KV<String, Long>, Long>() {
                private static final String EXPIRY_STATE_NAME = "expiry";

                @TimerId(EXPIRY_STATE_NAME)
                private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                @ProcessElement
                public void processElement(ProcessContext processContext, IntervalWindow window,
                                           @TimerId(EXPIRY_STATE_NAME) Timer expiryTimer) {
                    expiryTimer.set(window.maxTimestamp().plus(Duration.standardSeconds(5)));
                }

                @OnTimer(EXPIRY_STATE_NAME)
                public void expiryState(
                        OnTimerContext context) {
                }

            }));
            pipeline.run().waitUntilFinish();
        }).isInstanceOf(Pipeline.PipelineExecutionException.class)
                .hasMessage("java.lang.IllegalArgumentException: Attempted to set event time timer " +
                        "for 1970-01-01T00:00:19.999Z but that is after the " +
                        "expiration of window 1970-01-01T00:00:14.999Z");
    }

    @Test
    public void should_save_accumulated_entries_after_processing_time_timer() {
        Pipeline pipeline = BeamFunctions.createPipeline("Processing time timer expiration");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a1", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a1", 2L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b1", 5L), SEC_1_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(
                    TimestampedValue.of(KV.of("a2", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a2", 2L), SEC_1_DURATION),
                    TimestampedValue.of(KV.of("b2", 5L), SEC_1_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(
                    TimestampedValue.of(KV.of("a3", 1L), SEC_5_DURATION), TimestampedValue.of(KV.of("a3", 2L), SEC_5_DURATION),
                    TimestampedValue.of(KV.of("b3", 5L), SEC_5_DURATION)
            )
            // Need to advance 2 seconds to see the last entries flushed
            .advanceProcessingTime(Duration.standardSeconds(2))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(10);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<Iterable<String>> results = pipeline.apply(words).apply(window).apply(ParDo.of(new DoFn<KV<String, Long>, Iterable<String>>() {
            private static final String FLUSH_STATE_NAME = "expiry";
            private static final String ACCUMULATOR_NAME = "accumulator";

            @StateId(ACCUMULATOR_NAME)
            private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

            @TimerId(FLUSH_STATE_NAME)
            private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(ProcessContext processContext, IntervalWindow window,
                                       @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator,
                                       @TimerId(FLUSH_STATE_NAME) Timer expiryTimer) {
                if (wordsAccumulator.isEmpty().read()) {
                    expiryTimer.align(Duration.standardSeconds(1)).setRelative();
                }
                wordsAccumulator.add(processContext.element().getKey());
            }

            @OnTimer(FLUSH_STATE_NAME)
            public void flushAccumulatedResults(
                    OnTimerContext context,
                    @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator) {
                context.output(wordsAccumulator.read());
            }

        }));
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(10)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder(Arrays.asList("a1", "a1"),
                Arrays.asList("b1"), Arrays.asList("b2"), Arrays.asList("b3"), Arrays.asList("a3", "a3"),
                Arrays.asList("a2", "a2")
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_not_call_timer_when_it_planned_after_then_end_of_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Processing time timer triggers after the window ends");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a1", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a1", 2L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b1", 5L), SEC_1_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(10))
            .addElements(
                    TimestampedValue.of(KV.of("a12", 1L), SEC_12_DURATION), TimestampedValue.of(KV.of("a12", 2L), SEC_12_DURATION),
                    TimestampedValue.of(KV.of("b12", 5L), SEC_12_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(10))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(10);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<Iterable<String>> results = pipeline.apply(words).apply(window).apply(ParDo.of(new DoFn<KV<String, Long>, Iterable<String>>() {
            private static final String FLUSH_STATE_NAME = "expiry";
            private static final String ACCUMULATOR_NAME = "accumulator";

            @StateId(ACCUMULATOR_NAME)
            private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

            @TimerId(FLUSH_STATE_NAME)
            private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

            @ProcessElement
            public void processElement(ProcessContext processContext, IntervalWindow window,
                                       @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator,
                                       @TimerId(FLUSH_STATE_NAME) Timer expiryTimer) {
                if (wordsAccumulator.isEmpty().read()) {
                    // trigger after 15 seconds while the window is 10 seconds
                    expiryTimer.align(windowDuration.plus(Duration.standardSeconds(5))).setRelative();
                }
                wordsAccumulator.add(processContext.element().getKey());
            }

            @OnTimer(FLUSH_STATE_NAME)
            public void flushAccumulatedResults(
                    OnTimerContext context,
                    @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator) {
                context.output(wordsAccumulator.read());
            }

        }));
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(10)));
        // As we can see the result is empty because the timer based on processing time was defined to trigger
        // after the end of the window
        // Apparently we'd think that the data will be flushed in the 2nd window (processing timer planned for the
        // 15'' of processing). However it doesn't occur because the state is bounded to the window.
        PAssert.that(results).inWindow(window1).empty();
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_apply_timer_on_global_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Timely transform in global window");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        KvCoder<String, String> keyValueCoder = KvCoder.of(utf8Coder, utf8Coder);
        TestStream<KV<String, String>> usersWithVisitedPageX = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("user1", "page1"), SEC_1_DURATION), TimestampedValue.of(KV.of("user2", "page2"), SEC_1_DURATION),
                TimestampedValue.of(KV.of("user1", "page5"), SEC_1_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(10))
            .addElements(
                    TimestampedValue.of(KV.of("user2", "page11"), SEC_12_DURATION), TimestampedValue.of(KV.of("user3", "page21"), SEC_12_DURATION),
                    TimestampedValue.of(KV.of("user3", "page8"), SEC_12_DURATION)
            )
            .advanceProcessingTime(Duration.standardSeconds(10))
            .advanceWatermarkToInfinity();

        PCollection<KV<String, String>> usersWithVisitedPage = pipeline.apply(Create.of(Arrays.asList(
                KV.of("user1", "page1"), KV.of("user2", "page2"), KV.of("user1", "page5"),
                KV.of("user2", "page11"), KV.of("user3", "page21"), KV.of("user3", "page8")
        )));
        Window<KV<String, String>> globalWindow = Window.<KV<String, String>>into(new GlobalWindows()).triggering(Repeatedly
                .forever(AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(5))
                )
            )
            .withAllowedLateness(Duration.ZERO).discardingFiredPanes();
        PCollection<Long> userWithVisitsNumber = pipeline.apply(usersWithVisitedPageX).apply(globalWindow).apply(
                ParDo.of(new DoFn<KV<String, String>, Long>() {
            private static final String DATA_HOLDER_NAME = "sumState";

            private static final String EXPIRY_STATE_NAME = "expiry";

            @StateId(DATA_HOLDER_NAME)
            private final StateSpec<ValueState<Long>> sumStateSpec = StateSpecs.value(VarLongCoder.of());

            @TimerId(EXPIRY_STATE_NAME)
            private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext processContext,
                                       @StateId(DATA_HOLDER_NAME) ValueState<Long> sumState,
                                       @TimerId(EXPIRY_STATE_NAME) Timer expiryTimer) {
                long currentCounter = Optional.ofNullable(sumState.read()).orElse(0L);
                if (currentCounter == 0L) {
                    expiryTimer.align(Duration.standardSeconds(5)).setRelative();
                }
                long newCounterValue = currentCounter + 1;
                sumState.write(newCounterValue);
            }

            @OnTimer(EXPIRY_STATE_NAME)
            public void flushAccumulatedResults(
                    OnTimerContext context,
                    @StateId(DATA_HOLDER_NAME) ValueState<Long> sumState) {
                context.output(sumState.read());
            }
        }));

        // Here we deal with global window so we don't need to fear that the processing time timer won't be
        // called when the window expires.
        PAssert.that(userWithVisitsNumber).containsInAnyOrder(2L, 2L, 2L);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_invoke_trigger_at_event_time() {
        Pipeline pipeline = BeamFunctions.createPipeline("Event time timer expiration");
        Coder<String> utf8Coder = StringUtf8Coder.of();
        Coder<Long> varLongCoder = VarLongCoder.of();
        KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
        TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
                TimestampedValue.of(KV.of("a1", 1L), SEC_1_DURATION), TimestampedValue.of(KV.of("a1", 2L), SEC_1_DURATION),
                TimestampedValue.of(KV.of("b1", 5L), SEC_1_DURATION)
            )
            .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(2)))
            .addElements(
                TimestampedValue.of(KV.of("a2", 1L), SEC_3_DURATION), TimestampedValue.of(KV.of("a2", 2L), SEC_3_DURATION),
                TimestampedValue.of(KV.of("b2", 5L), SEC_3_DURATION)
            )
            .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(4)))
            .addElements(
                TimestampedValue.of(KV.of("a3", 1L), SEC_5_DURATION), TimestampedValue.of(KV.of("a3", 2L), SEC_5_DURATION),
                TimestampedValue.of(KV.of("b3", 5L), SEC_5_DURATION)
            )
            .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(2);
        Window<KV<String, Long>> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<Iterable<String>> results = pipeline.apply(words).apply(window).apply(
                ParDo.of(new DoFn<KV<String, Long>, Iterable<String>>() {
            private static final String FLUSH_STATE_NAME = "expiry";
            private static final String ACCUMULATOR_NAME = "accumulator";

            @StateId(ACCUMULATOR_NAME)
            private final StateSpec<BagState<String>> accumulatorStateSpec = StateSpecs.bag();

            @TimerId(FLUSH_STATE_NAME)
            private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            @ProcessElement
            public void processElement(ProcessContext processContext, IntervalWindow window,
                                       @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator,
                                       @TimerId(FLUSH_STATE_NAME) Timer expiryTimer) {
                if (wordsAccumulator.isEmpty().read()) {
                    expiryTimer.set(NOW.plus(Duration.standardSeconds(1)));
                }
                wordsAccumulator.add(processContext.element().getKey());
            }

            @OnTimer(FLUSH_STATE_NAME)
            public void flushAccumulatedResults(
                    OnTimerContext context,
                    @StateId(ACCUMULATOR_NAME) BagState<String> wordsAccumulator) {
                context.output(wordsAccumulator.read());
            }
        }));

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(2)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder(Arrays.asList("a1", "a1"),
                Arrays.asList("b1"));
        IntervalWindow window2 = new IntervalWindow(NOW.plus(Duration.standardSeconds(2)),
                NOW.plus(Duration.standardSeconds(4)));
        PAssert.that(results).inWindow(window2).containsInAnyOrder(Arrays.asList("a2", "a2"),
                Arrays.asList("b2"));
        IntervalWindow window3 = new IntervalWindow(NOW.plus(Duration.standardSeconds(4)),
                NOW.plus(Duration.standardSeconds(6)));
        PAssert.that(results).inWindow(window3).containsInAnyOrder(Arrays.asList("a3", "a3"),
                Arrays.asList("b3"));
        pipeline.run().waitUntilFinish();
    }
}
