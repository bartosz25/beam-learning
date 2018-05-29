package com.waitingforcode.pcollection;


import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collection;

@RunWith(JUnit4.class)
public class WindowTransformationsTest implements Serializable {

    @Test
    public void oooxoooxooo() throws Exception {
        Pipeline pipeline = BeamFunctions.createPipeline("Empty words filter");
        Instant windowNow = Instant.now();
        Instant windowNowMinus1 = Instant.now().minus(1);
        Instant windowNowMinus2 = Instant.now().minus(2);
        PCollection<Integer> timestampedNumbers = pipeline.apply(Create.timestamped(
                TimestampedValue.of(1, windowNowMinus2),
                TimestampedValue.of(2, windowNowMinus2),
                TimestampedValue.of(3, windowNowMinus2),
                TimestampedValue.of(4, windowNowMinus1),
                TimestampedValue.of(5, windowNowMinus1),
                TimestampedValue.of(6, windowNowMinus1),
                TimestampedValue.of(7, windowNowMinus1),
                TimestampedValue.of(11, windowNow),
                TimestampedValue.of(12, windowNow),
                TimestampedValue.of(13, windowNow)
        ));


        PCollection<String> mappedResult = timestampedNumbers.apply(
                Window.into(FixedWindows.of(new Duration(1)))
        ).apply(ParDo.of(new DoFn<Integer, String>() {
            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window) {
                System.out.println("Processing element" + c.element() + "for pane " + c.pane() +
                        "; at timestamp=" + c.timestamp() + " with window " + window);
                c.output("Got" + c.element());
            }
        }));
//        PAssert.that(mappedResult).inWindow(new IntervalWindow())

        //PCollection<Integer> windowedIntegers = numbers.apply(Window.into(new WindowingFunction()));

        //PCollection<Integer> fixedWindowNumbers = numbers.apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))));
        /*long now = Instant.now().getMillis();
        long nowPlus1H = now + 60 * 60 * 1000;

        Map<IntervalWindow, Set<String>> resultsPerWindow =
                runWindowFn(SlidingWindows.of(new Duration(7)).every(new Duration(5)), Arrays.asList(now, nowPlus1H));
        System.out.println(">>>> " + resultsPerWindow);*/
        pipeline.run();

    }
/*

    @Test
    public void uuuu() {
        Pipeline pipeline = BeamFunctions.createPipeline("With timestamp transformation");
        PCollection<Integer> timestampedNumbers = pipeline.apply(Create.timestamped(
                TimestampedValue.of(1, new Instant(1)),
                TimestampedValue.of(2, new Instant(1)),
                TimestampedValue.of(3, new Instant(1)),
                TimestampedValue.of(11, Instant.now()),
                TimestampedValue.of(12, Instant.now()),
                TimestampedValue.of(13, Instant.now())
        ));

        PCollection<Integer> windowedNumbers = timestampedNumbers.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        PAssert.that(windowedNumbers).containsInAnyOrder(1, 2, 3, 11, 12, 13);
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void xxxxo() {
        Pipeline pipeline = BeamFunctions.createPipeline("With timestamp transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("a", "b")));


    }

    @Test
    public void xxx() {
        PipelineOptions options = PipelineOptionsFactory.create();
        //options.setRunner(PipelineOptions.DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> readText = pipeline.apply(TextIO.read().from("/tmp/beam/*"));
        PCollection<String> extractedWords = readText.apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                // \p{L} denotes the category of Unicode letters,
                // so this pattern will match on everything that is not a letter.
                c.output(c.element());
            }
        }));
        PCollection<KV<String, Long>> countedWords = extractedWords.apply(Count.<String>perElement());

        DoFn<KV<String, Long>, String> outputFunction = new DoFn<KV<String, Long>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("KV="+c.element());
            }
        };
        countedWords.apply("FormatResults", ParDo.of(outputFunction));

        pipeline.run().waitUntilFinish();
    }
    class WindowedSum extends PTransform<PCollection<Integer>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<Integer> input) {
        return null;
    }

    private final class FormatCountsDoFn extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            c.output(
                    c.element().getKey()
                            + ":" + c.element().getValue()
                            + ":" + c.timestamp().getMillis()
                            + ":" + window);
        }
    }
}

    */
}

class WindowingFunction extends WindowFn<Integer, IntervalWindow> {

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext assignContext) throws Exception {
        return null;
    }

    @Override
    public void mergeWindows(MergeContext mergeContext) throws Exception {

    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return false;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return null;
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        return null;
    }
}
