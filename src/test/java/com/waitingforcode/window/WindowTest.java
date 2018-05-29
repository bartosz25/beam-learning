package com.waitingforcode.window;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class WindowTest implements Serializable  {

    @Test
    public void should_construct_fixed_time_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Fixed-time window");
        PCollection<String> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("a1", new Instant(1)), TimestampedValue.of("a3", new Instant(1)),
                TimestampedValue.of("a2", new Instant(1)), TimestampedValue.of("b1", new Instant(2)),
                TimestampedValue.of("a4", new Instant(1)), TimestampedValue.of("c1", new Instant(3)),
                TimestampedValue.of("a6", new Instant(1)), TimestampedValue.of("d1", new Instant(4)),
                TimestampedValue.of("d2", new Instant(4)), TimestampedValue.of("a5", new Instant(1))
        )));
        PCollection<String> windowedLetters = timestampedLetters.apply(Window.into(FixedWindows.of(new Duration(1))));
        windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.FIXED_TIME)));

        pipeline.run().waitUntilFinish();

        Map<Instant, List<String>> itemsPerWindow = WindowHandlers.FIXED_TIME.getItemsPerWindow();
        List<String> itemsInstant1 = itemsPerWindow.get(new Instant(1));
        assertThat(itemsInstant1).hasSize(6).containsOnly("a1", "a2", "a3", "a4", "a5", "a6");
        List<String> itemsInstant2 = itemsPerWindow.get(new Instant(2));
        assertThat(itemsInstant2).hasSize(1).containsOnly("b1");
        List<String> itemsInstant3 = itemsPerWindow.get(new Instant(3));
        assertThat(itemsInstant3).hasSize(1).containsOnly("c1");
        List<String> itemsInstant4 = itemsPerWindow.get(new Instant(4));
        assertThat(itemsInstant4).hasSize(2).containsOnly("d1", "d2");
    }

    @Test
    public void should_construct_sliding_time_window_with_duplicated_items() {
        Pipeline pipeline = BeamFunctions.createPipeline("Sliding-time window with duplicated items");
        PCollection<String> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("a1", new Instant(1)), TimestampedValue.of("a2", new Instant(1)),
                TimestampedValue.of("a3", new Instant(1)), TimestampedValue.of("b1", new Instant(2)),
                TimestampedValue.of("c1", new Instant(3)), TimestampedValue.of("d1", new Instant(4)),
                TimestampedValue.of("d2", new Instant(4)), TimestampedValue.of("a4", new Instant(1))
        )));

        PCollection<String> windowedLetters = timestampedLetters
                .apply(Window.into(SlidingWindows.of(new Duration(2)).every(new Duration(1))));
        windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.SLIDING_TIME_DUPLICATED)));

        pipeline.run().waitUntilFinish();

        Map<Instant, List<String>> itemsPerWindow = WindowHandlers.SLIDING_TIME_DUPLICATED.getItemsPerWindow();
        List<String> itemsInstant0 = itemsPerWindow.get(new Instant(0));
        assertThat(itemsInstant0).hasSize(4).containsOnly("a1", "a2", "a3", "a4");
        List<String> itemsInstant1 = itemsPerWindow.get(new Instant(1));
        assertThat(itemsInstant1).hasSize(5).containsOnly("a1", "a2", "a3", "a4", "b1");
        List<String> itemsInstant2 = itemsPerWindow.get(new Instant(2));
        assertThat(itemsInstant2).hasSize(2).containsOnly("b1", "c1");
        List<String> itemsInstant3 = itemsPerWindow.get(new Instant(3));
        assertThat(itemsInstant3).hasSize(3).containsOnly("c1", "d1", "d2");
        List<String> itemsInstant4 = itemsPerWindow.get(new Instant(4));
        assertThat(itemsInstant4).hasSize(2).containsOnly("d1", "d2");
    }

    @Test
    public void should_construct_sliding_time_window_with_missing_items() {
        Pipeline pipeline = BeamFunctions.createPipeline("Sliding-time window with missing items");
        PCollection<String> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("a1", new Instant(1)), TimestampedValue.of("a2", new Instant(1)),
                TimestampedValue.of("a3", new Instant(1)), TimestampedValue.of("b1", new Instant(2)),
                TimestampedValue.of("c1", new Instant(3)), TimestampedValue.of("d1", new Instant(4)),
                TimestampedValue.of("d2", new Instant(4)), TimestampedValue.of("a4", new Instant(1))
        )));

        PCollection<String> windowedLetters = timestampedLetters
                .apply(Window.into(SlidingWindows.of(new Duration(1)).every(new Duration(2))));
        windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.SLIDING_TIME_MISSING)));

        pipeline.run().waitUntilFinish();

        Map<Instant, List<String>> itemsPerWindow = WindowHandlers.SLIDING_TIME_MISSING.getItemsPerWindow();
        List<String> itemsInstant2 = itemsPerWindow.get(new Instant(2));
        assertThat(itemsInstant2).hasSize(1).containsOnly("b1");
        List<String> itemsInstant4 = itemsPerWindow.get(new Instant(4));
        assertThat(itemsInstant4).hasSize(2).containsOnly("d1", "d2");
    }

    @Test
    public void should_construct_calendar_based_window() throws InterruptedException {
        Pipeline pipeline = BeamFunctions.createPipeline("Calendar-based window");
        Instant day01012017 = DateTime.parse("2017-01-01T11:20").toInstant();
        Instant day01052017 = DateTime.parse("2017-05-01T11:20").toInstant();
        Instant day01062017 = DateTime.parse("2017-06-01T11:20").toInstant();
        Instant day02062017 = DateTime.parse("2017-06-02T11:20").toInstant();
        Instant day01072017 = DateTime.parse("2017-07-01T11:20").toInstant();
        Instant day02072017 = DateTime.parse("2017-07-02T11:20").toInstant();
        Instant day01122017 = DateTime.parse("2017-12-01T11:20").toInstant();
        Instant day31122017 = DateTime.parse("2017-12-31T11:20").toInstant();
        Instant day01012018 = DateTime.parse("2018-01-01T11:20").toInstant();
        PCollection<String> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("01012017", day01012017), TimestampedValue.of("01052017", day01052017),
                TimestampedValue.of("01062017", day01062017), TimestampedValue.of("01072017", day01072017),
                TimestampedValue.of("01122017", day01122017), TimestampedValue.of("31122017", day31122017),
                TimestampedValue.of("01012018", day01012018), TimestampedValue.of("02062017", day02062017),
                TimestampedValue.of("02072017", day02072017)
        )));

        CalendarWindows.MonthsWindows monthsWindow = CalendarWindows.months(6).beginningOnDay(1).withStartingMonth(2017, 1);
        PCollection<String> windowedLetters = timestampedLetters.apply(Window.into(monthsWindow));
        windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.CALENDAR)));

        pipeline.run().waitUntilFinish();
        Map<Instant, List<String>> itemsPerWindow = WindowHandlers.CALENDAR.getItemsPerWindow();
        assertThat(itemsPerWindow).hasSize(3);
        List<String> itemsWindow1 = itemsPerWindow.get(DateTime.parse("2017-01-01T00:00:00.000Z").toInstant());
        assertThat(itemsWindow1).hasSize(4).containsOnly("01012017", "01052017", "01062017", "02062017");
        List<String> itemsWindow2 = itemsPerWindow.get(DateTime.parse("2017-07-01T00:00:00.000Z").toInstant());
        assertThat(itemsWindow2).hasSize(4).containsOnly("01122017", "01072017", "31122017", "02072017");
        List<String> itemsWindow3 = itemsPerWindow.get(DateTime.parse("2018-01-01T00:00:00.000Z"));
        assertThat(itemsWindow3).hasSize(1).containsOnly("01012018");
    }

    @Test
    public void should_construct_session_window_for_key_value_elements() {
        Pipeline pipeline = BeamFunctions.createPipeline("Session window example for key-value elements");
        PCollection<KV<String, String>> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(KV.of("a", "a1"), new Instant(1)), TimestampedValue.of(KV.of("a", "a2"), new Instant(1)),
                TimestampedValue.of(KV.of("a", "a3"), new Instant(5)), TimestampedValue.of(KV.of("b", "b1"), new Instant(2)),
                TimestampedValue.of(KV.of("c", "c1"), new Instant(3)), TimestampedValue.of(KV.of("d", "d1"), new Instant(4)),
                TimestampedValue.of(KV.of("d", "d2"), new Instant(4)), TimestampedValue.of(KV.of("a", "a4"), new Instant(5))
        )));

        PCollection<KV<String, String>> windowedLetters = timestampedLetters
                .apply(Window.into(Sessions.withGapDuration(new Duration(2))));

        IntervalWindow window1 = new IntervalWindow(new Instant(1), new Instant(3));
        PAssert.that(windowedLetters).inWindow(window1).containsInAnyOrder(KV.of("a", "a1"), KV.of("a", "a2"));
        IntervalWindow window2 = new IntervalWindow(new Instant(2), new Instant(4));
        PAssert.that(windowedLetters).inWindow(window2).containsInAnyOrder(KV.of("b", "b1"));
        IntervalWindow window3 = new IntervalWindow(new Instant(3), new Instant(5));
        PAssert.that(windowedLetters).inWindow(window3).containsInAnyOrder(KV.of("c", "c1"));
        IntervalWindow window4 = new IntervalWindow(new Instant(4), new Instant(6));
        PAssert.that(windowedLetters).inWindow(window4).containsInAnyOrder(KV.of("d", "d1"), KV.of("d", "d2"));
        // As you can see, the session is key-based. Normally the [a3, a4] fits into window4. However, as their
        // window is constructed accordingly to "a" session, i.e. 1-3, 4 (no data), 5 - 7, they go to the window 5 - 7
        // and not 4 - 6
        IntervalWindow window5 = new IntervalWindow(new Instant(5), new Instant(7));
        PAssert.that(windowedLetters).inWindow(window5).containsInAnyOrder(KV.of("a", "a3"), KV.of("a", "a4"));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_construct_global_window_for_key_value_elements() {
        Pipeline pipeline = BeamFunctions.createPipeline("Global window example for key-value elements");
        PCollection<KV<String, String>> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(KV.of("a", "a1"), new Instant(1)), TimestampedValue.of(KV.of("a", "a2"), new Instant(1)),
                TimestampedValue.of(KV.of("a", "a3"), new Instant(5)), TimestampedValue.of(KV.of("b", "b1"), new Instant(2)),
                TimestampedValue.of(KV.of("c", "c1"), new Instant(3)), TimestampedValue.of(KV.of("d", "d1"), new Instant(4)),
                TimestampedValue.of(KV.of("d", "d2"), new Instant(4)), TimestampedValue.of(KV.of("a", "a4"), new Instant(5))
        )));

        PCollection<KV<String, String>> windowedLetters = timestampedLetters
                .apply(Window.into(new GlobalWindows()));

        PAssert.that(windowedLetters).inWindow(GlobalWindow.INSTANCE).containsInAnyOrder(KV.of("a", "a1"),
                KV.of("a", "a2"), KV.of("a", "a3"), KV.of("a", "a4"), KV.of("b", "b1"), KV.of("c", "c1"),
                KV.of("d", "d1"), KV.of("d", "d2"));
        pipeline.run().waitUntilFinish();
    }
}

class DataPerWindowHandler extends DoFn<String, String> {

    private WindowHandlers windowHandler;

    public DataPerWindowHandler(WindowHandlers windowHandler) {
        this.windowHandler = windowHandler;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext, BoundedWindow window) throws InterruptedException {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        windowHandler.addItem(processContext.element(), intervalWindow.start());
        // Add a small sleep - otherwise test results are not deterministic
        Thread.sleep(300);
        processContext.output(processContext.element());
    }

}

enum WindowHandlers {

    FIXED_TIME, SLIDING_TIME_DUPLICATED, SLIDING_TIME_MISSING, CALENDAR;

    private Map<Instant, List<String>> itemsPerWindow = new ConcurrentHashMap<Instant, List<String>>();

    public void addItem(String item, Instant windowInterval) {
        List<String> windowItems = itemsPerWindow.getOrDefault(windowInterval, new ArrayList<>());
        windowItems.add(item);
        itemsPerWindow.put(windowInterval, windowItems);
    }

    public Map<Instant, List<String>> getItemsPerWindow() {
        return itemsPerWindow;
    }

}