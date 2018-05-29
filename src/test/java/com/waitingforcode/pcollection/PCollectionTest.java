package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class PCollectionTest implements Serializable {

    private static final String FILE_1 = "/tmp/beam/1";
    private static final String FILE_2 = "/tmp/beam/2";

    @BeforeClass
    public static void writeFiles() throws IOException {
        FileUtils.writeStringToFile(new File(FILE_1), "1\n2\n3\n4", "UTF-8");
        FileUtils.writeStringToFile(new File(FILE_2), "5\n6\n7\n8", "UTF-8");
    }

    @AfterClass
    public static void deleteFiles() {
        FileUtils.deleteQuietly(new File(FILE_1));
        FileUtils.deleteQuietly(new File(FILE_2));
    }

    @Test
    public void should_construct_pcollection_from_memory_objects() {
        List<String> letters = Arrays.asList("a", "b", "c", "d");
        Pipeline pipeline = BeamFunctions.createPipeline("Creating PCollection from memory");

        PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));

        PAssert.that(lettersCollection).containsInAnyOrder("a", "b", "c", "d");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_construct_pcollection_without_applying_transformation_on_it() {
        Pipeline pipeline = BeamFunctions.createPipeline("Creating PCollection from file");

        TextIO.Read reader = TextIO.read().from("/tmp/beam/*");
        PCollection<String> readNumbers = pipeline.apply(reader);

        PAssert.that(readNumbers).containsInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_not_modify_input_pcollection_after_applying_the_transformation() {
        List<String> letters = Arrays.asList("a", "b", "c", "d");
        Pipeline pipeline = BeamFunctions.createPipeline("Transforming a PCollection");

        PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
        PCollection<String> aLetters = lettersCollection.apply(Filter.equal("a"));

        PAssert.that(lettersCollection).containsInAnyOrder("a", "b", "c", "d");
        PAssert.that(aLetters).containsInAnyOrder("a");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_use_one_pcollection_as_input_for_different_transformations() {
        List<String> letters = Arrays.asList("a", "b", "c", "d");
        Pipeline pipeline = BeamFunctions.createPipeline("Using one PCollection as input for different transformations");

        PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
        PCollection<String> aLetters = lettersCollection.apply(Filter.equal("a"));
        PCollection<String> notALetter = lettersCollection.apply(Filter.greaterThan("a"));

        PAssert.that(lettersCollection).containsInAnyOrder("a", "b", "c", "d");
        PAssert.that(aLetters).containsInAnyOrder("a");
        PAssert.that(notALetter).containsInAnyOrder("b", "c", "d");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_create_explicitly_timestamped_batch_pcollection_with_custom_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Creating timestamped collection");
        PCollection<Integer> timestampedNumbers = pipeline.apply(Create.timestamped(
                TimestampedValue.of(1, new Instant(1)),
                TimestampedValue.of(2, new Instant(2)),
                TimestampedValue.of(3, new Instant(3)),
                TimestampedValue.of(4, new Instant(4))
        ));

        PCollection<String> mappedResult = timestampedNumbers.apply(
                Window.into(FixedWindows.of(new Duration(1)))
        ).apply(ParDo.of(new DoFn<Integer, String>() {
            @ProcessElement
            public void processElement(ProcessContext processingContext, BoundedWindow window) {
                processingContext.output(processingContext.element() + "(" + processingContext.timestamp() + ")" +
                    " window="+window
                );
            }
        }));

        PAssert.that(mappedResult).containsInAnyOrder(
                "1(1970-01-01T00:00:00.001Z) window=[1970-01-01T00:00:00.001Z..1970-01-01T00:00:00.002Z)",
                "2(1970-01-01T00:00:00.002Z) window=[1970-01-01T00:00:00.002Z..1970-01-01T00:00:00.003Z)",
                "3(1970-01-01T00:00:00.003Z) window=[1970-01-01T00:00:00.003Z..1970-01-01T00:00:00.004Z)",
                "4(1970-01-01T00:00:00.004Z) window=[1970-01-01T00:00:00.004Z..1970-01-01T00:00:00.005Z)");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_pcollection_coder() {
        List<String> letters = Arrays.asList("a", "b", "c", "d");
        Pipeline pipeline = BeamFunctions.createPipeline("PCollection coder");

        PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
        pipeline.run().waitUntilFinish();

        Coder<String> lettersCoder = lettersCollection.getCoder();
        assertThat(lettersCoder.getClass()).isEqualTo(StringUtf8Coder.class);
    }

    @Test
    public void should_get_pcollection_metadata() {
        List<String> letters = Arrays.asList("a", "b", "c", "d");
        Pipeline pipeline = BeamFunctions.createPipeline("PCollection metadata");

        PCollection<String> lettersCollection = pipeline.apply("A-B-C-D letters", Create.of(letters));
        pipeline.run().waitUntilFinish();

        assertThat(lettersCollection.isBounded()).isEqualTo(PCollection.IsBounded.BOUNDED);
        WindowingStrategy<?, ?> windowingStrategy = lettersCollection.getWindowingStrategy();
        assertThat(windowingStrategy.getWindowFn().getClass()).isEqualTo(GlobalWindows.class);
        assertThat(lettersCollection.getName()).isEqualTo("A-B-C-D letters/Read(CreateSource).out");
    }

    @Test
    public void should_create_pcollection_list() {
        Pipeline pipeline = BeamFunctions.createPipeline("PCollection list");
        PCollection<String> letters1 = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
        PCollection<String> letters2 = pipeline.apply(Create.of(Arrays.asList("d", "e", "f")));
        PCollection<String> letters3 = pipeline.apply(Create.of(Arrays.asList("g", "h", "i")));


        PCollectionList<String> allLetters = PCollectionList.of(letters1).and(letters2).and(letters3);
        List<PCollection<String>> lettersCollections = allLetters.getAll();

        PAssert.that(lettersCollections.get(0)).containsInAnyOrder("a", "b", "c");
        PAssert.that(lettersCollections.get(1)).containsInAnyOrder("d", "e", "f");
        PAssert.that(lettersCollections.get(2)).containsInAnyOrder("g", "h", "i");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_create_pcollection_tuple() {
        Pipeline pipeline = BeamFunctions.createPipeline("PCollection tuple");
        PCollection<String> letters = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
        PCollection<Integer> numbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
        PCollection<Boolean> flags = pipeline.apply(Create.of(Arrays.asList(true, false, true)));
        TupleTag<String> lettersTag = new TupleTag<>();
        TupleTag<Integer> numbersTag = new TupleTag<>();
        TupleTag<Boolean> flagsTag = new TupleTag<>();

        PCollectionTuple mixedData = PCollectionTuple.of(lettersTag, letters).and(numbersTag, numbers).and(flagsTag, flags);
        Map<TupleTag<?>, PCollection<?>> tupleData = mixedData.getAll();

        PAssert.that((PCollection<String>)tupleData.get(lettersTag)).containsInAnyOrder("a", "b", "c");
        PAssert.that((PCollection<Integer>)tupleData.get(numbersTag)).containsInAnyOrder(1, 2, 3);
        PAssert.that((PCollection<Boolean>)tupleData.get(flagsTag)).containsInAnyOrder(true, false, true);
        pipeline.run().waitUntilFinish();
    }
}
