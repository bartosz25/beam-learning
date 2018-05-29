package com.waitingforcode.pcollection;

import com.google.common.base.MoreObjects;
import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnit4.class)
public class SideOutputTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private static final Instant SEC_2_DURATION = NOW.plus(Duration.standardSeconds(2));
    private static final Instant SEC_3_DURATION = NOW.plus(Duration.standardSeconds(3));

    @Test
    public void should_build_collections_of_2_different_types() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side output with 2 different types");
        TupleTag<String> stringBooleans = new TupleTag<String>(){};
        TupleTag<Integer> integerBooleans = new TupleTag<Integer>(){};

        PCollection<Boolean> booleanFlags = pipeline.apply(Create.of(Arrays.asList(true, true, false, false, true)));

        PCollectionTuple outputDatasets = booleanFlags.apply(ParDo.of(new DoFn<Boolean, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                processContext.output(processContext.element().toString());
                processContext.output(integerBooleans, getNumberFromBoolean(processContext.element()));
            }
            private int getNumberFromBoolean(boolean flag) {
                return flag ? 1 : 0;
            }
        }).withOutputTags(stringBooleans, TupleTagList.of(integerBooleans)));

        PCollection<String> stringFlags = outputDatasets.get(stringBooleans);
        PCollection<Integer> integerFlags = outputDatasets.get(integerBooleans);
        PAssert.that(stringFlags).containsInAnyOrder("true", "true", "false", "false", "true");
        PAssert.that(integerFlags).containsInAnyOrder(1, 1, 0, 0, 1);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_build_collections_of_the_same_type_but_for_different_processing_later() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side output branching processing");
        TupleTag<Letter> validLetters = new TupleTag<Letter>(){};
        TupleTag<Letter> invalidLetters = new TupleTag<Letter>(){};
        PCollection<Letter> booleanFlags = pipeline.apply(Create.of(Arrays.asList(
                new Letter("a"), new Letter("b"), new Letter(""),
                new Letter("c"), new Letter("d")
        )));

        PCollectionTuple outputDatasets = booleanFlags.apply(ParDo.of(new DoFn<Letter, Letter>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                Letter processedLetter = processContext.element();
                if (processedLetter.isValid()) {
                    processContext.output(processedLetter);
                } else {
                    processContext.output(invalidLetters, processedLetter);
                }
            }
        }).withOutputTags(validLetters, TupleTagList.of(invalidLetters)));

        PCollection<Letter> validLettersPCollection = outputDatasets.get(validLetters);
        validLettersPCollection.apply(ParDo.of(new DoFn<Letter, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                // Sample store taking valid letters
                LetterStore.VALID.addLetter(processContext.element());
            }
        }));
        PCollection<Letter> invalidLettersPCollection = outputDatasets.get(invalidLetters);
        invalidLettersPCollection.apply(ParDo.of(new DoFn<Letter, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                // Sample store taking invalid letters
                LetterStore.INVALID.addLetter(processContext.element());
            }
        }));
        pipeline.run().waitUntilFinish();

        List<Letter> validLettersFromStore = LetterStore.VALID.getData();
        assertThat(validLettersFromStore).extracting("letter").containsOnly("a", "b", "c", "d");
        List<Letter> invalidLettersFromStore = LetterStore.INVALID.getData();
        assertThat(invalidLettersFromStore).extracting("letter").containsOnly("");
    }

    @Test
    public void should_allow_to_build_empty_side_output() {
        Pipeline pipeline = BeamFunctions.createPipeline("Empty side output", 2);
        TestStream<String> letters = TestStream.create(StringUtf8Coder.of()).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("b", SEC_1_DURATION),
                TimestampedValue.of("c", SEC_1_DURATION), TimestampedValue.of("d", SEC_1_DURATION),
                TimestampedValue.of("e", SEC_1_DURATION), TimestampedValue.of("f", SEC_1_DURATION)
        )
                .advanceWatermarkToInfinity();
        TupleTag<String> notEmptyLetters = new TupleTag<String>(){};
        TupleTag<String> emptyLetters = new TupleTag<String>(){};
        PCollectionTuple outputDatasets = pipeline.apply(letters).apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) throws InterruptedException {
            }
        }).withOutputTags(notEmptyLetters, TupleTagList.of(emptyLetters)));

        PCollection<String> notEmptyLettersDataset = outputDatasets.get(notEmptyLetters);
        notEmptyLettersDataset.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {}
        }));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_show_that_declaring_side_output_without_bracets_should_not_work() {
        Pipeline pipeline = BeamFunctions.createPipeline("TupleTag side output declaration as not anonymous class (no {})");
        TupleTag<String> stringBooleans = new TupleTag<>();
        TupleTag<Integer> integerBooleans = new TupleTag<>();

        PCollection<Boolean> booleanFlags = pipeline.apply(Create.of(Arrays.asList(true, true, false, false, true)));

        assertThatThrownBy(() -> {
            PCollectionTuple outputDatasets = booleanFlags.apply(ParDo.of(new DoFn<Boolean, String>() {
                @ProcessElement
                public void processElement(ProcessContext processContext) {
                }
            }).withOutputTags(stringBooleans, TupleTagList.of(integerBooleans)));
            pipeline.run().waitUntilFinish();
        })
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to return a default Coder for ParMultiDo(Anonymous).out1 [PCollection]. " +
                "Correct one of the following root causes")
        .hasMessageContaining("Inferring a Coder from the CoderRegistry failed: Unable to provide a Coder for V.")
        .hasMessageContaining("No Coder has been manually specified;  you may do so using .setCoder().");
    }

    @Test
    public void should_compute_different_side_outputs_in_different_windows() {
        Pipeline pipeline = BeamFunctions.createPipeline("Different side outputs in 2 different windows");
        TestStream<String> letters = TestStream.create(StringUtf8Coder.of()).addElements(
                TimestampedValue.of("a", SEC_1_DURATION), TimestampedValue.of("b", SEC_1_DURATION),
                TimestampedValue.of("f", SEC_1_DURATION), TimestampedValue.of("c", SEC_1_DURATION),
                TimestampedValue.of("d", SEC_2_DURATION), TimestampedValue.of("e", SEC_3_DURATION)
            )
            .advanceWatermarkToInfinity();

        TupleTag<String> lettersRepeatedOnce = new TupleTag<String>(){};
        TupleTag<String> lettersRepeatedTwice = new TupleTag<String>(){};
        Duration windowDuration = Duration.standardSeconds(2);
        Window<String> window = Window.<String>into(FixedWindows.of(windowDuration)) ; // .withAllowedLateness(Duration.ZERO, Window.ClosingBehavior.FIRE_ALWAYS) .accumulatingFiredPanes();
        PCollectionTuple outputDatasets = pipeline.apply(letters).apply(window)
            .apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext processContext, IntervalWindow window) {
                    String letter = processContext.element();
                    processContext.output(letter);
                    String repeatedTwiceLetter = letter+letter;
                    processContext.output(lettersRepeatedTwice, repeatedTwiceLetter);
                }
            }).withOutputTags(lettersRepeatedOnce, TupleTagList.of(lettersRepeatedTwice)));

        IntervalWindow window1 = new IntervalWindow(NOW, NOW.plus(windowDuration));
        PAssert.that(outputDatasets.get(lettersRepeatedOnce)).inFinalPane(window1)
                .containsInAnyOrder("a", "b", "c", "f");
        PAssert.that(outputDatasets.get(lettersRepeatedTwice)).inFinalPane(window1)
                .containsInAnyOrder("aa", "bb", "cc", "ff");
        IntervalWindow window2 = new IntervalWindow(NOW.plus(windowDuration), NOW.plus(windowDuration).plus(windowDuration));
        PAssert.that(outputDatasets.get(lettersRepeatedOnce)).inFinalPane(window2)
                .containsInAnyOrder("d", "e");
        PAssert.that(outputDatasets.get(lettersRepeatedTwice)).inFinalPane(window2)
                .containsInAnyOrder("dd", "ee");
        pipeline.run().waitUntilFinish();
    }

}

enum LetterStore {
    VALID, INVALID;

    private List<Letter> data = new ArrayList<>();

    public void addLetter(Letter letter) {
        data.add(letter);
    }

    public List<Letter> getData() {
        return data;
    }

}

class Letter implements Serializable {
    private final String letter;
    private final boolean valid;

    public Letter(String letter) {
        this.letter = letter;
        if (this.letter.isEmpty()) {
            valid = false;
        } else {
            valid = true;
        }
    }

    public String getLetter() {
        return letter;
    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("letter", letter).add("valid", valid).toString();
    }
}