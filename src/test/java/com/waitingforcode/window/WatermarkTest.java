package com.waitingforcode.window;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
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
public class WatermarkTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private static final Instant SEC_2_DURATION = NOW.plus(Duration.standardSeconds(2));

    @Test
    public void should_show_unobservably_late_data() {
        Pipeline pipeline = BeamFunctions.createPipeline("Unobservably late data");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> lettersStream = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_2_DURATION)
        )
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .withAllowedLateness(Duration.standardSeconds(5), Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, lettersStream);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=3");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_discard_late_data_with_default_configuration() {
        Pipeline pipeline = BeamFunctions.createPipeline("Discarded late data");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> lettersStream = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION)
        )
        // the window is now+5 sec, advance to 6'' and add one late event that should be discarded
        .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.into(FixedWindows.of(windowDuration));

        PCollection<String> results = applyCounter(pipeline, window, lettersStream);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=3");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_accept_late_data_with_allowed_lateness_and_accumulation_mode() {
        // It's an example of observably late data, i.e. late data arriving after the end of the window but before
        // the allowed lateness time elapses
        Pipeline pipeline = BeamFunctions.createPipeline("Accepted late data with accepted previous panes");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> lettersStream = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION)
        )
        // the window is now+5 sec, advance to 6'' and add one late event that should be accepted
        .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(6)))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(11)))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                // closing behavior defines the conditions under which the final pane will be computed
                // FIRE_ALWAYS fires the creation always - even if there is not new data since the last firing
                .withAllowedLateness(Duration.standardSeconds(5), Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, lettersStream);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=4");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_accept_late_data_but_discard_the_data_after_watermark() {
        // This is an example of droppably late data, i.e. data arriving after the window and allowed lateness
        Pipeline pipeline = BeamFunctions.createPipeline("Accepted late data but only within watermark");
        Coder<String> utfCoder = StringUtf8Coder.of();
        TestStream<String> lettersStream = TestStream.create(utfCoder).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("a", SEC_1_DURATION),
                TimestampedValue.of("a", SEC_1_DURATION)
        )
        // the window is now+5 sec, advance to 15'' and add one late event that should be discarded
        .advanceWatermarkTo(NOW.plus(Duration.standardSeconds(15)))
        .addElements(TimestampedValue.of("a", SEC_1_DURATION))
        .advanceWatermarkToInfinity();
        Duration windowDuration = Duration.standardSeconds(5);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration))
                .withAllowedLateness(Duration.standardSeconds(5), Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes();

        PCollection<String> results = applyCounter(pipeline, window, lettersStream);

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(Duration.standardSeconds(5)));
        PAssert.that(results).inOnTimePane(window1).containsInAnyOrder("a=3");
        PAssert.that(results).inFinalPane(window1).containsInAnyOrder("a=3");
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
