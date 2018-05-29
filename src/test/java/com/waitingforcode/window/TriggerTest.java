package com.waitingforcode.window;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

@RunWith(JUnit4.class)
public class TriggerTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));

    @Test
    public void should_emit_early_results_after_receiving_6_events() {
        Pipeline pipeline = BeamFunctions.createPipeline("Early results after 6 events received");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION)
        )
        .addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(AfterPane.elementCountAtLeast(3))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        // doc: "Triggers allow Beam to emit early results, before all the data in a given window has arrived.
        // For example, emitting after a certain amount of time elapses, or after a certain number of elements arrives."
        // Here we want to emit the results after the window receives at least 3 items. In this case we compute the
        // result for 2 .addElements operations (2 + 4 items). The last .addElements is ignored
        // See the next test to discover what happens if we add only 1 event in .addElements method
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=6");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=6");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_early_results_after_receiving_3_events() {
        Pipeline pipeline = BeamFunctions.createPipeline("Early results after 3 events received");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(AfterPane.elementCountAtLeast(3))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=3");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=3");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_early_results_after_processing_time_elapsed() {
        Pipeline pipeline = BeamFunctions.createPipeline("Early results after processing time elapsed");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION)
        )
        // processing time trigger fires the pane after 2 seconds, add 3 seconds then check if the final
        // pane contains the data after this time
        .advanceProcessingTime(Duration.standardSeconds(3))
        .addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(AfterProcessingTime.pastFirstElementInPane().alignedTo(Duration.standardSeconds(2)))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=2");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=2");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_the_results_after_processing_time_aligned_to_date() {
        Pipeline pipeline = BeamFunctions.createPipeline("Processing time trigger aligned to a date");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                // Advance processing time to 3 seconds - normally the additional elements should be included
                // in the result (even if the processing time trigger is of 2 seconds). The processing time trigger
                // is aligned to the date NOW + 1 second, so it behaves as we'd advance the processing time to only 2 sec
                .advanceProcessingTime(Duration.standardSeconds(3))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        AfterProcessingTime processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane()
                .alignedTo(Duration.standardSeconds(2), NOW.plus(Duration.standardSeconds(1)));
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(processingTimeTrigger)
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=12");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=12");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_results_after_watermark_passed_when_trigger_is_never() {
        Pipeline pipeline = BeamFunctions.createPipeline("Results triggered with Never trigger");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION)
        )
        // Advance processing time to show that it hasn't the influence on Never trigger + watermark
        .advanceProcessingTime(Duration.standardSeconds(3))
        .addElements(
                TimestampedValue.of("a", SEC_1_DURATION),TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION),TimestampedValue.of("a", SEC_1_DURATION))
        // Now advance the watermark to the out of the window to show that the last "a" element is discarded
        .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(20)))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(Never.ever())
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        // Using Never trigger means that the result is computed at window_end + watermark time
        // That said this type of trigger is never used - the pane firing is based on window + watermark time
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=6");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=6");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_discard_items_accumulated_in_previous_pane() {
        Pipeline pipeline = BeamFunctions.createPipeline("Discard already fired panes");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION)
            )
            // advance to 6 sec to see late event included in the final result
            .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        AfterWatermark.AfterWatermarkEarlyAndLate afterWatermark = AfterWatermark.pastEndOfWindow()
                .withLateFirings(AfterProcessingTime.pastFirstElementInPane())
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane());
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(afterWatermark)
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .discardingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        // a=1 since we discard already fired panes
        // See the next test to discover the difference between discarding and accumulating fired panes
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=1");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_accumulate_items_accumulated_in_previous_pane() {
        Pipeline pipeline = BeamFunctions.createPipeline("Accumulate already fired panes");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION)
            )
            // advance to 6 sec to see late event included in the final result
            .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        AfterWatermark.AfterWatermarkEarlyAndLate afterWatermark = AfterWatermark.pastEndOfWindow()
                .withLateFirings(AfterProcessingTime.pastFirstElementInPane())
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane());
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(afterWatermark)
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        // a=4 since we accumulate already fired panes
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=4");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_the_results_with_early_and_late_sub_triggers_for_watermark() {
        Pipeline pipeline = BeamFunctions.createPipeline("Early and late fired panes with watermark");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION)
                )
                // Advance to 5 secs - allowed processing time is 2 secs, so it should emit the early results
                .advanceProcessingTime(Duration.standardSeconds(5))
                // add new elements and advance watermark and add 3x3 elements to see if 9 or 2 elements
                // are in the final pane
                .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        AfterWatermark.AfterWatermarkEarlyAndLate afterWatermark = AfterWatermark.pastEndOfWindow()
                .withLateFirings(AfterPane.elementCountAtLeast(2))
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2)));
        Window<String> window = Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(afterWatermark)
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=9");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_the_results_with_at_least_trigger_even_if_the_threshold_was_not_reached() {
        Pipeline pipeline = BeamFunctions.createPipeline("atLeast trigger threshold not reached");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION)
        )
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(AfterPane.elementCountAtLeast(20))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // At least trigger is not strict, i.e. even if the number of items is not reached, it'll
        // trigger the pane
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=6");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=6");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_emit_the_results_after_the_first_executed_trigger() {
        Pipeline pipeline = BeamFunctions.createPipeline("The first launched trigger");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            // Advance processing time in order to execute the processingTimeTrigger instead of elementsCountTrigger
            // defined below
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        AfterPane elementsCountTrigger = AfterPane.elementCountAtLeast(20);
        AfterProcessingTime processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(2));
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(AfterFirst.of(elementsCountTrigger, processingTimeTrigger))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);
        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // Only 6 items are in the pane since the trigger computes the window either after accumulating 20 items or
        // 2 seconds after receiving the first element
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=6");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=6");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_fire_final_pane_after_all_triggers() {
        Pipeline pipeline = BeamFunctions.createPipeline("After all triggers pane firing");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                // Advance processing time - the processing time trigger should be fired
                .advanceProcessingTime(Duration.standardSeconds(20))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(AfterAll.of(AfterPane.elementCountAtLeast(12),
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2))))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=12");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=12");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_build_composite_trigger_with_stopping_condition() {
        Pipeline pipeline = BeamFunctions.createPipeline("Composite trigger with stopping condition");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            // advance the processing time to 5 from 10 acceptable seconds
            .advanceProcessingTime(Duration.standardSeconds(5))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION))
            // advance processing time to the 13th second - the items above should be discarded
            .advanceProcessingTime(Duration.standardSeconds(7))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Trigger.OnceTrigger mainTrigger = AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(10));
        Trigger.OnceTrigger untilTrigger = AfterPane.elementCountAtLeast(2);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(mainTrigger.orFinally(untilTrigger))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // Here we use OrFinallyTrigger that stops the execution of main trigger when the conditional trigger
        // (here until trigger) is fired
        // See the next test to discover what happens without untilTrigger
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=2");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=2");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_show_what_happens_for_orfinallytrigger_without_stopping_condition() {
        Pipeline pipeline = BeamFunctions.createPipeline("As composite trigger but without stopping condition");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            // advance the processing time to 5 from 10 acceptable seconds
            .advanceProcessingTime(Duration.standardSeconds(5))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION))
            // advance processing time to the 13th second - the items above should be discarded
            .advanceProcessingTime(Duration.standardSeconds(7))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION)
            )
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Trigger.OnceTrigger processingTimeTrigger = AfterProcessingTime
                .pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10));
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(processingTimeTrigger)
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        // Unlike previous test here we don't use the stopping condition. Obviously, the main trigger
        // executes normally and accumulates 9 'a' items
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=9");
        PAssert.that(results).inWindow(window1).containsInAnyOrder("a=9");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_repeatedly_execute_elements_count_based_trigger() {
        Pipeline pipeline = BeamFunctions.createPipeline("Elements count based trigger executed repeatedly");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(
                    TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
            .addElements(TimestampedValue.of("a", SEC_1_DURATION))
            .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=9");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_repeatedly_execute_elements_count_based_trigger_for_global_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Elements count based trigger executed repeatedly on global window");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> onTimeLetters = TestStream.create(utfCoder)
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(
                        TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION))
                .addElements(TimestampedValue.of("a", SEC_1_DURATION))
                .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                .withAllowedLateness(windowDuration, Window.ClosingBehavior.FIRE_ALWAYS)
                .discardingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, onTimeLetters);

        GlobalWindow globalWindow = GlobalWindow.INSTANCE;
        PAssert.that(results).inFinalPane(globalWindow).containsInAnyOrder("a=1");
        pipeline.run().waitUntilFinish();
    }

    private static PCollection<String> applyCounter(Pipeline pipeline, Window<String> window,
                                                    TestStream<String> inputCollection) {
        return pipeline.apply(inputCollection)
                .apply(window)
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String letter) -> KV.of(letter, 1)))
                .apply(Count.perKey())
                .apply(MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> pair) ->
                        pair.getKey() + "=" + pair.getValue()));
    }

}
