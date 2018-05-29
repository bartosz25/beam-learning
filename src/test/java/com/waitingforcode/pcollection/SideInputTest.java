package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnit4.class)
public class SideInputTest implements Serializable {

    private static final Instant NOW = new Instant(0);
    private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));
    private static final Instant SEC_2_DURATION = NOW.plus(Duration.standardSeconds(2));
    private static final Instant SEC_3_DURATION = NOW.plus(Duration.standardSeconds(3));

    @Test
    public void should_show_side_input_in_global_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side input in global window");
        PCollection<Integer> processedMeanNumbers = pipeline.apply(Create.of(Arrays.asList(2, 50, 20)));
        // Here the parameter shared by all functions must be computed
        PCollectionView<Integer> minNumber = processedMeanNumbers.apply(Min.integersGlobally().asSingletonView());
        PCollection<Integer> processedNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));

        PCollection<Integer> numbersFromChars = processedNumbers.apply(
                ParDo.of(new DoFn<Integer, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        int minNumberToFilter = processContext.sideInput(minNumber);
                        int processedElement = processContext.element();
                        if (processedElement > minNumberToFilter) {
                            processContext.output(processedElement);
                        }
                    }
                }).withSideInputs(minNumber));

        PAssert.that(numbersFromChars).containsInAnyOrder(3, 4, 5, 6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_compute_different_side_inputs_with_streaming_processing() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side inputs with windows for unbounded collection");
        PCollection<String> orders = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("order#1", SEC_1_DURATION), TimestampedValue.of("order#2", SEC_1_DURATION),
                TimestampedValue.of("order#3", SEC_2_DURATION), TimestampedValue.of("order#4", SEC_2_DURATION)
        )));
        PCollection<Boolean> paymentSystemErrors = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(false, SEC_1_DURATION), TimestampedValue.of(true, SEC_2_DURATION)
        )));
        // It's illegal to define side input window lower than the data window; It returns an error
        // with empty PCollectionView
        Window<Boolean> paymentSystemErrorsWindow = Window.into(FixedWindows.of(Duration.standardSeconds(2)));
        Window<String> processingWindow = Window.into(FixedWindows.of(Duration.standardSeconds(1)));

        PCollectionView<Boolean> paymentErrorsPerSecond = paymentSystemErrors
                .apply(paymentSystemErrorsWindow).apply(View.asSingleton());

        // In order to filter we need to pass by ParDo - it doesn't apply to Filter.by(...)
        PCollection<String> ordersWithoutPaymentProblem = orders.apply(processingWindow).apply(
                ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                    boolean wasPaymentError = processContext.sideInput(paymentErrorsPerSecond);
                    if (!wasPaymentError) {
                        processContext.output(processContext.element());
                    }
            }
        }).withSideInputs(paymentErrorsPerSecond));

        IntervalWindow window1 = new IntervalWindow(SEC_1_DURATION, SEC_2_DURATION);
        PAssert.that(ordersWithoutPaymentProblem).inWindow(window1).containsInAnyOrder("order#1", "order#2");
        IntervalWindow window2 = new IntervalWindow(SEC_2_DURATION, SEC_3_DURATION);
        PAssert.that(ordersWithoutPaymentProblem).inWindow(window2).empty();
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_fail_when_side_input_window_is_bigger_than_processing_window_and_more_than_1_value_is_found() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side inputs with too big window");
        PCollection<String> orders = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("order#1", SEC_1_DURATION), TimestampedValue.of("order#2", SEC_1_DURATION),
                TimestampedValue.of("order#3", SEC_2_DURATION), TimestampedValue.of("order#4", SEC_2_DURATION)
        )));
        PCollection<Boolean> paymentSystemErrors = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(false, SEC_1_DURATION), TimestampedValue.of(true, SEC_2_DURATION)
        )));
        Window<Boolean> paymentSystemErrorsWindow = Window.into(FixedWindows.of(Duration.standardSeconds(20)));
        Window<String> processingWindow = Window.into(FixedWindows.of(Duration.standardSeconds(1)));
        assertThatThrownBy(() -> {
            PCollectionView<Boolean> paymentErrorsPerSecond = paymentSystemErrors
                    .apply(paymentSystemErrorsWindow).apply(View.asSingleton());
            orders.apply(processingWindow).apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext processContext) {
                    boolean wasPaymentError = processContext.sideInput(paymentErrorsPerSecond);
                    if (!wasPaymentError) {
                        processContext.output(processContext.element());
                    }
                }
            }).withSideInputs(paymentErrorsPerSecond));
            pipeline.run().waitUntilFinish();
        })
        .isInstanceOf(Pipeline.PipelineExecutionException.class)
        .hasMessageContaining("PCollection with more than one element accessed as a singleton view. " +
                "Consider using Combine.globally().asSingleton() to combine the PCollection into a single value");

    }

    @Test
    public void should_fail_when_the_window_of_side_input_is_smaller_than_the_processing_window() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side input window smaller than processing window");
        PCollection<String> orders = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("order#1", SEC_1_DURATION), TimestampedValue.of("order#2", SEC_1_DURATION),
                TimestampedValue.of("order#3", SEC_2_DURATION), TimestampedValue.of("order#4", SEC_2_DURATION)
        )));
        PCollection<Boolean> paymentSystemErrors = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(false, SEC_1_DURATION), TimestampedValue.of(true, SEC_2_DURATION)
        )));
        Window<Boolean> paymentSystemErrorsWindow = Window.into(FixedWindows.of(Duration.standardSeconds(1)));
        Window<String> processingWindow = Window.into(FixedWindows.of(Duration.standardSeconds(5)));

        assertThatThrownBy(() -> {
            PCollectionView<Boolean> paymentErrorsPerSecond = paymentSystemErrors
                    .apply(paymentSystemErrorsWindow).apply(View.asSingleton());
            orders.apply(processingWindow).apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext processContext) {
                    boolean wasPaymentError = processContext.sideInput(paymentErrorsPerSecond);
                    if (!wasPaymentError) {
                        processContext.output(processContext.element());
                    }
                }
            }).withSideInputs(paymentErrorsPerSecond));

            pipeline.run().waitUntilFinish();
        }).isInstanceOf(Pipeline.PipelineExecutionException.class)
                .hasMessageContaining("Empty PCollection accessed as a singleton view");
    }

    @Test
    public void should_show_that_side_input_brings_precedence_execution_rule_when_it_s_overriden_after_pardo_using_it() {
        Pipeline pipeline = BeamFunctions.createPipeline("Side inputs with different windows");
        PCollection<String> orders = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of("order#1", SEC_1_DURATION), TimestampedValue.of("order#2", SEC_1_DURATION),
                TimestampedValue.of("order#3", SEC_2_DURATION), TimestampedValue.of("order#4", SEC_2_DURATION)
        )));
        PCollection<Boolean> paymentSystemErrors = pipeline.apply(Create.timestamped(Arrays.asList(
                TimestampedValue.of(false, SEC_1_DURATION), TimestampedValue.of(true, SEC_2_DURATION)
        )));
        // It's illegal to define side input window lower than the data window; It returns an error
        // with empty PCollectionView
        Window<Boolean> paymentSystemErrorsWindow = Window.into(FixedWindows.of(Duration.standardSeconds(2)));
        Window<String> processingWindow = Window.into(FixedWindows.of(Duration.standardSeconds(1)));
        assertThatThrownBy(() -> {
            PCollectionView<Boolean> paymentErrorsPerSecond = null;
            orders.apply(processingWindow).apply(ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext processContext) {
                }
            }).withSideInputs(paymentErrorsPerSecond));

            paymentErrorsPerSecond = paymentSystemErrors.apply(paymentSystemErrorsWindow).apply(View.asSingleton());
            pipeline.run().waitUntilFinish();
        }).isInstanceOf(NullPointerException.class);
    }

}