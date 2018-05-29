package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class ParDoTest implements Serializable  {

    @Test
    public void should_show_no_global_state_existence() {
        Pipeline pipeline = BeamFunctions.createPipeline("Global state not existence test");
        PCollection<Integer> numbers123 = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
        class IdentityFunction extends DoFn<Integer, Integer> {
            private final List<Integer> numbers;

            public IdentityFunction(List<Integer> numbers) {
                this.numbers = numbers;
            }

            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                processContext.output(processContext.element());
                numbers.add(processContext.element());
            }

            @DoFn.Teardown
            public void teardown() {
                if (numbers.isEmpty()) {
                    throw new IllegalStateException("Numbers on worker should not be empty");
                }
                System.out.println("numbers="+numbers);
            }

        }
        List<Integer> numbers = new ArrayList<>();

        PCollection<Integer> numbersFromChars = numbers123.apply(ParDo.of(new IdentityFunction(numbers)));

        PAssert.that(numbersFromChars).containsInAnyOrder(1, 2, 3);
        assertThat(numbers).isEmpty();
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_compute_singleton_pcollection_as_shared_state() {
        Pipeline pipeline = BeamFunctions.createPipeline("PCollection singleton state test");
        PCollection<Integer> processedMeanNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));
        // Here the parameter shared by all functions must be computed
        PCollectionView<Integer> minNumber = processedMeanNumbers.apply(Min.integersGlobally().asSingletonView());
        PCollection<Integer> processedNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));
        class MinNumberFilter extends DoFn<Integer, Integer> {

            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) throws InterruptedException {
                int minNumberProcessed = processContext.sideInput(minNumber);
                if (processContext.element() > minNumberProcessed*2) {
                    processContext.output(processContext.element());
                }
            }
        }

        PCollection<Integer> numbersFromChars = processedNumbers.apply(
                ParDo.of(new MinNumberFilter()).withSideInputs(minNumber));

        PAssert.that(numbersFromChars).containsInAnyOrder(3, 4, 5, 6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_compute_shared_state_in_functions_constructor() {
        int workers = 5;
        Pipeline pipeline = BeamFunctions.createPipeline("Shared state from function constructor test", workers);
        PCollection<Integer> processedNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));
        class IdentityFunction extends DoFn<Integer, Integer> {

            public IdentityFunction() {
                Counter.INSTANCE.incrementConstructorCalls();
            }

            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                processContext.output(processContext.element());
            }

            @DoFn.Teardown
            public void tearDown() {
                Counter.INSTANCE.incrementTeardownCalls();
            }
        }
        List<Integer> numbers = new ArrayList<>();

        PCollection<Integer> numbersFromChars = processedNumbers.apply(ParDo.of(new IdentityFunction()));

        PAssert.that(numbersFromChars).containsInAnyOrder(1, 2, 3, 4, 5, 6);
        assertThat(numbers).isEmpty();
        pipeline.run().waitUntilFinish();
        // As show, the constructor was called only once and teardown function more than that (it proves the
        // presence of more than 1 function's instance)
        // It shows that we can also initialize a state shared by different functions in the constructor
        // It also shows that the function's object is initialized once and sent serialized to the workers
        assertThat(Counter.INSTANCE.getConstructorCalls()).isEqualTo(1);
        assertThat(Counter.INSTANCE.getTeardownCalls()).isEqualTo(workers);
    }

    @Test
    public void should_show_lifecycle() {
        Pipeline pipeline = BeamFunctions.createPipeline("Lifecycle test", 2);
        PCollection<Integer> numbers1To100 =
                pipeline.apply(Create.of(IntStream.rangeClosed(1, 1_000).boxed().collect(Collectors.toList())));
        class LifecycleHandler extends DoFn<Integer, Integer> {

            @DoFn.Setup
            public void setup() {
                LifecycleEventsHandler.INSTANCE.addSetup();
            }

            @DoFn.StartBundle
            public void initMap(DoFn<Integer, Integer>.StartBundleContext startBundleContext) {
                LifecycleEventsHandler.INSTANCE.addStartBundle();
            }

            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                processContext.output(processContext.element());
                LifecycleEventsHandler.INSTANCE.addProcessing();
            }

            @DoFn.FinishBundle
            public void finishBundle(DoFn<Integer, Integer>.FinishBundleContext finishBundleContext) {
                LifecycleEventsHandler.INSTANCE.addFinishBundle();
            }

            @DoFn.Teardown
            public void turnOff(){
                LifecycleEventsHandler.INSTANCE.addTeardown();
            }

        }

        PCollection<Integer> processedNumbers = numbers1To100.apply(ParDo.of(new LifecycleHandler()));

        pipeline.run().waitUntilFinish();
        assertThat(LifecycleEventsHandler.INSTANCE.getSetupCount()).isEqualTo(2);
        // The number of bundles is not fixed over the time but it'd vary between 2 and 4
        // It proves however that the @StartBundle method doesn't means the same as @Setup one
        assertThat(LifecycleEventsHandler.INSTANCE.getStartBundleCount()).isGreaterThanOrEqualTo(2);
        assertThat(LifecycleEventsHandler.INSTANCE.getProcessingCount()).isEqualTo(1_000);
        assertThat(LifecycleEventsHandler.INSTANCE.getFinishBundleCount()).isEqualTo(LifecycleEventsHandler.INSTANCE.getStartBundleCount());
        assertThat(LifecycleEventsHandler.INSTANCE.getTeardownCount()).isEqualTo(2);
    }

    @Test
    public void should_take_different_inputs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Different inputs test");
        PCollection<Integer> numbers1To5 = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5)));
        PCollection<KV<Integer, String>> numbersWords = pipeline.apply(Create.of(Arrays.asList(
            KV.of(1, "one"), KV.of(2, "two"), KV.of(3, "three"), KV.of(4, "four"), KV.of(5, "five")
        )));
        PCollectionView<Map<Integer, String>> numbersWordsView = numbersWords.apply(View.asMap());
        class ExternalMapper extends DoFn<Integer, String> {
            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                String word = processContext.sideInput(numbersWordsView).get(processContext.element());
                processContext.output(word);
            }
        }

        PCollection<String> mappedNumbers = numbers1To5.apply(ParDo.of(new ExternalMapper())
        .withSideInputs(Collections.singleton(numbersWordsView)));

        PAssert.that(mappedNumbers).containsInAnyOrder("one", "two", "three", "four", "five");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_produce_different_outputs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Different outputs test", 2);
        PCollection<Integer> numbers1To5 = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5)));
        TupleTag<Integer> multipliedNumbersTag = new TupleTag<Integer>(){};
        TupleTag<String> originalNumbersTags = new TupleTag<String>(){};
        class Multiplicator extends DoFn<Integer, Integer> {
            @DoFn.ProcessElement
            public void processElement(ProcessContext processContext) {
                int number = processContext.element();
                processContext.output(originalNumbersTags, ""+number);
                processContext.output(number*2);
            }
        }

        PCollectionTuple results = numbers1To5.apply(ParDo.of(new Multiplicator())
                .withOutputTags(multipliedNumbersTag, TupleTagList.of(originalNumbersTags)));

        PCollection<Integer> multipliedNumbersEntries = results.get(multipliedNumbersTag);
        PAssert.that(multipliedNumbersEntries).containsInAnyOrder(2, 4, 6, 8, 10);
        PCollection<String> originalNumbersEntries = results.get(originalNumbersTags);
        PAssert.that(originalNumbersEntries).containsInAnyOrder("1", "2", "3", "4", "5");
        pipeline.run().waitUntilFinish();
    }
}


enum LifecycleEventsHandler {
    INSTANCE;

    private AtomicInteger setupCalls = new AtomicInteger(0);

    private AtomicInteger startBundleCalls = new AtomicInteger(0);

    private AtomicInteger processingCalls = new AtomicInteger(0);

    private AtomicInteger finishBundleCalls = new AtomicInteger(0);

    private AtomicInteger teardownCalls = new AtomicInteger(0);

    public void addSetup() {
        setupCalls.incrementAndGet();
    }

    public int getSetupCount() {
        return setupCalls.get();
    }

    public void addStartBundle() {
        startBundleCalls.incrementAndGet();
    }

    public int getStartBundleCount() {
        return startBundleCalls.get();
    }

    public void addProcessing() {
        processingCalls.incrementAndGet();
    }

    public int getProcessingCount() {
        return processingCalls.get();
    }

    public void addFinishBundle() {
        finishBundleCalls.incrementAndGet();
    }

    public int getFinishBundleCount() {
        return finishBundleCalls.get();
    }

    public void addTeardown() {
        teardownCalls.incrementAndGet();
    }

    public int getTeardownCount() {
        return teardownCalls.get();
    }
}

enum Counter {
    INSTANCE;

    private AtomicInteger counterConstructorCalls = new AtomicInteger(0);

    private AtomicInteger counterTeardownCalls = new AtomicInteger(0);

    public void incrementConstructorCalls() {
        counterConstructorCalls.incrementAndGet();
    }

    public void incrementTeardownCalls() {
        counterTeardownCalls.incrementAndGet();
    }

    public int getConstructorCalls() {
        return counterConstructorCalls.get();
    }

    public int getTeardownCalls() {
        return counterTeardownCalls.get();
    }
}

