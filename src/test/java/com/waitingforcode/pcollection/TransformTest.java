package com.waitingforcode.pcollection;

import com.google.common.collect.Iterables;
import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class TransformTest implements Serializable {

    @Test
    public void should_filter_empty_words() {
        Pipeline pipeline = BeamFunctions.createPipeline("Empty words filter");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab",
                "ab", "abc")));

        PCollection<String> notEmptyWords =
                dataCollection.apply(Filter.by(Filters.NOT_EMPTY));

        PAssert.that(notEmptyWords).containsInAnyOrder(Arrays.asList("a", "ab", "ab", "abc"));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_take_only_a_letters()  {
        Pipeline pipeline = BeamFunctions.createPipeline("'a' letters filter");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("a", "b", "c", "a", "d", "a")));

        PCollection<String> aLetters = dataCollection.apply(Filter.equal("a"));

        PAssert.that(aLetters).containsInAnyOrder(Arrays.asList("a", "a", "a"));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_keep_numbers_greater_or_equal_to_2() {
        Pipeline pipeline = BeamFunctions.createPipeline("Numbers greater or equal to 2 filter");
        PCollection<Integer> dataCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

        PCollection<Integer> numbersGreaterOrEqual2 = dataCollection.apply(Filter.greaterThanEq(2));

        PAssert.that(numbersGreaterOrEqual2).containsInAnyOrder(Arrays.asList(2, 3));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_map_words_into_their_length() {
        Pipeline pipeline = BeamFunctions.createPipeline("Mapping transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab",
                "ab", "abc")));

        PCollection<Integer> wordsLengths =
                dataCollection.apply(
                        MapElements.into(TypeDescriptors.integers()).via(word -> word.length()));

        PAssert.that(wordsLengths).containsInAnyOrder(Arrays.asList(0, 0, 0, 1, 2, 2, 3));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_partition_numbers_to_10_equal_partitions() {
        Pipeline pipeline = BeamFunctions.createPipeline("Partitioning transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(
                IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList())));

        PCollectionList<Integer> repartitionedNumbers = numbersCollection.apply(Partition.of(4,
                (Partition.PartitionFn<Integer>) (element, numPartitions) -> element % numPartitions));

        PAssert.that(repartitionedNumbers.get(0)).containsInAnyOrder(4, 8, 12, 16, 20);
        PAssert.that(repartitionedNumbers.get(1)).containsInAnyOrder(1, 5, 9, 13, 17);
        PAssert.that(repartitionedNumbers.get(2)).containsInAnyOrder(2, 6, 10, 14, 18);
        PAssert.that(repartitionedNumbers.get(3)).containsInAnyOrder(3, 7, 11, 15, 19);

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_count_the_number_of_elements_in_collection() {
        Pipeline pipeline = BeamFunctions.createPipeline("Count transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

        PCollection<Long> allItemsCount = numbersCollection.apply(Count.globally());

        PAssert.that(allItemsCount).containsInAnyOrder(3L);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_count_the_occurrences_per_element_in_key_value_collection() {
        Pipeline pipeline = BeamFunctions.createPipeline("Count per element transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab",
                "ab", "abc")));

        PCollection<KV<String, Long>> perElementCount = dataCollection.apply(Count.perElement());

        PAssert.that(perElementCount).containsInAnyOrder(KV.of("", 3L),
                KV.of("a", 1L), KV.of("ab", 2L), KV.of("abc", 1L));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_distinct_words() {
        Pipeline pipeline = BeamFunctions.createPipeline("Distinct transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab",
                "ab", "abc")));

        PCollection<String> dataCollectionWithoutDuplicates = dataCollection.apply(Distinct.create());

        PAssert.that(dataCollectionWithoutDuplicates).containsInAnyOrder("", "a", "ab", "abc");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_keep_only_one_pair() {
        Pipeline pipeline = BeamFunctions.createPipeline("Distinct with representative values for key-value pairs");
        PCollection<KV<Integer, String>> dataCollection = pipeline.apply(Create.of(Arrays.asList(
                KV.of(1, "a"), KV.of(2, "b"), KV.of(1, "a"), KV.of(10, "a")
        )));

        PCollection<KV<Integer, String>> distinctPairs = dataCollection.apply(
                Distinct.withRepresentativeValueFn(new SerializableFunction<KV<Integer, String>, String>() {
                    @Override
                    public String apply(KV<Integer, String> input) {
                        return input.getValue();
                    }
                }));

        PAssert.that(distinctPairs).containsInAnyOrder(KV.of(1, "a"), KV.of(2, "b"));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_min_value() {
        Pipeline pipeline = BeamFunctions.createPipeline("Min value transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

        PCollection<Integer> minValue = numbersCollection.apply(Min.globally());

        PAssert.that(minValue).containsInAnyOrder(1);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_min_value_per_customer_key() {
        Pipeline pipeline = BeamFunctions.createPipeline("Min value transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, Integer>> minCustomersAmount = customerOrders.apply(Min.perKey());

        PAssert.that(minCustomersAmount).containsInAnyOrder(KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_min_value_with_custom_comparator() {
        Pipeline pipeline = BeamFunctions.createPipeline("Min value transformation");
        PCollection<Integer> numbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));

        PCollection<Integer> minAndEven = numbers.apply(Min.globally(Comparators.LOWER_AND_EVEN));

        PAssert.that(minAndEven).containsInAnyOrder(2);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_max_value() {
        Pipeline pipeline = BeamFunctions.createPipeline("Max value transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6)));

        PCollection<Integer> maxValue = numbersCollection.apply(Max.globally());

        PAssert.that(maxValue).containsInAnyOrder(6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_max_value_per_customer_key() {
        Pipeline pipeline = BeamFunctions.createPipeline("Max value transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, Integer>> maxCustomerAmounts = customerOrders.apply(Max.perKey());

        PAssert.that(maxCustomerAmounts).containsInAnyOrder(KV.of("C#1", 210), KV.of("C#2", 450), KV.of("C#3", 120));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_max_value_with_custom_comparator() {
        Pipeline pipeline = BeamFunctions.createPipeline("Max value with custom comparator transformation");
        PCollection<Integer> numbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6, 7)));

        PCollection<Integer> maxAndEven = numbers.apply(Max.globally(Comparators.BIGGER_AND_EVEN));

        PAssert.that(maxAndEven).containsInAnyOrder(6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_mean_value_of_numeric_values() {
        Pipeline pipeline = BeamFunctions.createPipeline("Mean value transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5,
                6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)));

        PCollection<Double> meanValue = numbersCollection.apply(Mean.globally());

        PAssert.that(meanValue).containsInAnyOrder(10.5d);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_compute_mean_per_key() {
        Pipeline pipeline = BeamFunctions.createPipeline("Mean value transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, Double>> meanAmountPerCustomer = customerOrders.apply(Mean.perKey());

        PAssert.that(meanAmountPerCustomer).containsInAnyOrder(KV.of("C#1", 179.75d), KV.of("C#2", 279d),
                KV.of("C#3", 120d));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_sum_value_of_numeric_values() {
        Pipeline pipeline = BeamFunctions.createPipeline("Sum value transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));

        PCollection<Integer> integersSum = numbersCollection.apply(Sum.integersGlobally());

        PAssert.that(integersSum).containsInAnyOrder(6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_sum_value_per_key() {
        Pipeline pipeline = BeamFunctions.createPipeline("Sum value per key");
        PCollection<KV<String, Integer>> numbersCollection = pipeline.apply(Create.of(
                KV.of("A", 100), KV.of("A", 200), KV.of("B", 150), KV.of("A", 100)
        ));

        PCollection<KV<String, Integer>> keyedSum = numbersCollection.apply(Sum.integersPerKey());

        PAssert.that(keyedSum).containsInAnyOrder(KV.of("A", 400), KV.of("B", 150));
        pipeline.run().waitUntilFinish();

    }

    @Test
    public void should_get_the_first_2_elements() {
        Pipeline pipeline = BeamFunctions.createPipeline("Top 2 transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(IntStream.rangeClosed(1, 20)
                .boxed().collect(Collectors.toList())));

        PCollection<List<Integer>> top2Items = numbersCollection.apply(Top.largest(2));

        PAssert.that(top2Items).containsInAnyOrder(Arrays.asList(20, 19));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_the_last_2_elements() {
        Pipeline pipeline = BeamFunctions.createPipeline("Top 2 reversed transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(IntStream.rangeClosed(1, 20)
                .boxed().collect(Collectors.toList())));

        PCollection<List<Integer>> top2Items = numbersCollection.apply(Top.smallest(2));

        PAssert.that(top2Items).containsInAnyOrder(Arrays.asList(1, 2));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_2_first_values_per_key() {
        Pipeline pipeline = BeamFunctions.createPipeline("Top 2 per key transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, List<Integer>>> top2Orders = customerOrders.apply(Top.largestPerKey(2));

        PAssert.that(top2Orders).containsInAnyOrder(KV.of("C#1", Arrays.asList(210, 209)),
                KV.of("C#2", Arrays.asList(450, 108)),  KV.of("C#3", Collections.singletonList(120))
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_get_the_first_value_with_custom_comparator() {
        Pipeline pipeline = BeamFunctions.createPipeline("Top 1 custom comparator");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, List<Integer>>> topEvenAmounts =
                customerOrders.apply(Top.perKey(1, Comparators.BIGGER_AND_EVEN));

        PAssert.that(topEvenAmounts).containsInAnyOrder(KV.of("C#1", Arrays.asList(210)),
                KV.of("C#2", Arrays.asList(450)),  KV.of("C#3", Collections.singletonList(120))
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_retrieve_values_of_key_value_pairs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Amounts values");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<Integer> amounts = customerOrders.apply(Values.create());

        PAssert.that(amounts).containsInAnyOrder(100, 108, 120, 209, 210, 200, 450);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_retrieve_keys_of_key_value_pairs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Amounts keys");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<String> customerCodes = customerOrders.apply(Keys.create());

        PAssert.that(customerCodes).containsInAnyOrder("C#1", "C#1", "C#1", "C#1", "C#2", "C#2", "C#3");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_sample_3_numbers() {
        Pipeline pipeline = BeamFunctions.createPipeline("Sample transformation");
        PCollection<Integer> numbersCollection = pipeline.apply(Create.of(IntStream.rangeClosed(1, 20)
                .boxed().collect(Collectors.toList())));

        PCollection<Iterable<Integer>> sampledNumbers = numbersCollection.apply(Sample.fixedSizeGlobally(3));

        PAssert.that(sampledNumbers).satisfies(input -> {
            Set<Integer> distinctNumbers = new HashSet<>();
            for (Iterable<Integer> ints : input) {
                for (int number : ints) {
                    distinctNumbers.add(number);
                }
            }
            assertThat(distinctNumbers).hasSize(3);
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_group_orders_by_customer() {
        Pipeline pipeline = BeamFunctions.createPipeline("Group by key transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, Iterable<Integer>>> groupedOrders = customerOrders.apply(GroupByKey.create());

        PAssert.that(groupedOrders).satisfies(input -> {
            Map<String, List<Integer>> expected = new HashMap<>();
            expected.put("C#1", Arrays.asList(210, 200, 209, 100));
            expected.put("C#2", Arrays.asList(108, 450));
            expected.put("C#3", Arrays.asList(120));
            for (KV<String, Iterable<Integer>> keyValues : input) {
                List<Integer> expectedOrderAmounts = expected.get(keyValues.getKey());
                assertThat(keyValues.getValue()).containsOnlyElementsOf(expectedOrderAmounts);
            }
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_join_the_elements_of_2_collections() {
        Pipeline pipeline = BeamFunctions.createPipeline("Group by key transformation");
        PCollection<KV<String, Integer>> elements1 = pipeline.apply(Create.of(
                KV.of("A", 1), KV.of("B", 10), KV.of("A", 5), KV.of("A", 3), KV.of("B", 11)
        ));
        PCollection<KV<String, Integer>> elements2 = pipeline.apply(Create.of(
                KV.of("A", 6), KV.of("B", 12), KV.of("A", 4), KV.of("A", 2), KV.of("C", 20)
        ));
        TupleTag<Integer> tupleTag1 = new TupleTag<>();
        TupleTag<Integer> tupleTag2 = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> coGroupedElements =
                KeyedPCollectionTuple.of(tupleTag1, elements1).and(tupleTag2, elements2).apply(CoGroupByKey.create());


        PAssert.that(coGroupedElements).satisfies(input -> {
            Map<String, List<Integer>> expected = new HashMap<>();
            expected.put("A", Arrays.asList(1, 2, 3, 4, 5, 6));
            expected.put("B", Arrays.asList(10, 11, 12));
            expected.put("C", Arrays.asList(20));
            for (KV<String, CoGbkResult> result : input) {
                Iterable<Integer> allFrom1 = result.getValue().getAll(tupleTag1);
                Iterable<Integer> allFrom2 = result.getValue().getAll(tupleTag2);
                Iterable<Integer> groupedValues = Iterables.concat(allFrom1, allFrom2);
                assertThat(groupedValues).containsOnlyElementsOf(expected.get(result.getKey()));
            }
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_flat_map_numbers() {
        Pipeline pipeline = BeamFunctions.createPipeline("FlatMap transformation");
        PCollection<Integer> numbers = pipeline.apply(Create.of(1, 10, 100));

        PCollection<Integer> flattenNumbers = numbers.apply(FlatMapElements.into(TypeDescriptors.integers())
                .via(new Multiplicator(2)));

        PAssert.that(flattenNumbers).containsInAnyOrder(1, 2, 10, 20, 100, 200);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_flatten_numbers() {
        Pipeline pipeline = BeamFunctions.createPipeline("Flatten transformation");
        PCollection<List<Integer>> numbersFromList = pipeline.apply(Create.of(
                Arrays.asList(1, 2, 3, 4), Arrays.asList(10, 11, 12, 13), Arrays.asList(20, 21, 22, 23)
        ));

        PCollection<Integer> flattenNumbers = numbersFromList.apply(Flatten.iterables());

        PAssert.that(flattenNumbers).containsInAnyOrder(1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_flatten_numbers_from_list_pcollection() {
        Pipeline pipeline = BeamFunctions.createPipeline("Flatten transformation");
        PCollection<Integer> numbers1 = pipeline.apply(Create.of(1, 2, 3, 4));
        PCollection<Integer> numbers2 = pipeline.apply(Create.of(10, 11, 12, 13));
        PCollection<Integer> numbers3 = pipeline.apply(Create.of(20, 21, 22, 23));
        PCollectionList<Integer> numbersList = PCollectionList.of(numbers1).and(numbers2).and(numbers3);

        PCollection<Integer> flattenNumbers = numbersList.apply(Flatten.pCollections());

        PAssert.that(flattenNumbers).containsInAnyOrder(1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_find_1_occurrence_of_regex_expression() {
        Pipeline pipeline = BeamFunctions.createPipeline("RegEx find transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("", "a", "", "", "ab",
                "ab", "abc")));

        PCollection<String> foundWords = dataCollection.apply(Regex.find("(ab)"));

        PAssert.that(foundWords).containsInAnyOrder("ab", "ab", "ab");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_find_1_occurrence_of_regex_expression_per_group() {
        Pipeline pipeline = BeamFunctions.createPipeline("RegEx find in group transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(
                "aa ab c", "ab bb c", "ab cc d", "dada"
        ));

        PCollection<KV<String, String>> foundWords = dataCollection.apply(Regex.findKV("(ab) (c)", 1, 2));

        PAssert.that(foundWords).containsInAnyOrder(KV.of("ab", "c"), KV.of("ab", "c"));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_replace_all_a_by_1() {
        Pipeline pipeline = BeamFunctions.createPipeline("RegEx replaceAll transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("aa", "aba", "baba")));

        PCollection<String> replacedWords = dataCollection.apply(Regex.replaceAll("a", "1"));

        PAssert.that(replacedWords).containsInAnyOrder("11", "1b1", "b1b1");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_replace_first_match() {
        Pipeline pipeline = BeamFunctions.createPipeline("RegEx replaceAll transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("aa", "aaaa", "aba")));

        PCollection<String> replacedWords = dataCollection.apply(Regex.replaceFirst("a", "1"));

        PAssert.that(replacedWords).containsInAnyOrder("1a", "1aaa", "1ba");
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void should_split_sentences_by_whitespace() {
        Pipeline pipeline = BeamFunctions.createPipeline("RegEx replaceAll transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("aa bb cc", "aaaa aa cc", "aba bab")));

        PCollection<String> splittedWords = dataCollection.apply(Regex.split("\\s"));

        PAssert.that(splittedWords).containsInAnyOrder("aa", "bb", "cc", "aaaa", "aa", "cc", "aba", "bab");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_concat_all_words() {
        Pipeline pipeline = BeamFunctions.createPipeline("Combine transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("aa", "aa", "aaa")));

        PCollection<String> concatenatedWords = dataCollection.apply(Combine.globally(words -> {
            String concatenatedWord = "";
            for (String word : words) {
                concatenatedWord += word;
            }
            return concatenatedWord;
        }));

        PAssert.that(concatenatedWords).containsInAnyOrder("aaaaaaa");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_combine_all_orders_by_customer() {
        Pipeline pipeline = BeamFunctions.createPipeline("Combine per key transformation");
        PCollection<KV<String, Integer>> customerOrders = pipeline.apply(Create.of(Arrays.asList(
                KV.of("C#1", 100), KV.of("C#2", 108), KV.of("C#3", 120), KV.of("C#1", 209), KV.of("C#1", 210),
                KV.of("C#1", 200), KV.of("C#2", 450))));

        PCollection<KV<String, Integer>> ordersSumsPerCustomer = customerOrders.apply(Combine.perKey(amounts -> {
            int sum = 0;
            for (int amount : amounts) {
                sum += amount;
            }
            return sum;
        }));

        PAssert.that(ordersSumsPerCustomer).containsInAnyOrder(KV.of("C#1", 719), KV.of("C#2", 558), KV.of("C#3", 120));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_apply_timestamp_to_input_elements() {
        Pipeline pipeline = BeamFunctions.createPipeline("With timestamp transformation");
        PCollection<String> dataCollection = pipeline.apply(Create.of(Arrays.asList("a", "b")));

        Instant timestampToApply = Instant.now().minus(2 * 60 * 1000);
        PCollection<String> itemsWithNewTimestamp = dataCollection
                .apply(WithTimestamps.of(input -> timestampToApply));
        PCollection<Long> elementsTimestamps = itemsWithNewTimestamp.apply(ParDo.of(new DoFn<String, Long>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                processContext.output(processContext.timestamp().getMillis());
            }
        }));

        PAssert.that(elementsTimestamps).containsInAnyOrder(timestampToApply.getMillis(), timestampToApply.getMillis());
        pipeline.run().waitUntilFinish();
    }


}

class Multiplicator implements SerializableFunction<Integer, Collection<Integer>> {

    private int factor;

    public Multiplicator(int factor) {
        this.factor = factor;
    }

    @Override
    public Collection<Integer> apply(Integer number) {
        return Arrays.asList(number, factor*number);
    }
}

enum Comparators implements Comparator<Integer> {
    BIGGER_AND_EVEN {
        @Override
        public int compare(Integer comparedNumber, Integer toCompare) {
            boolean isEvenCompared = comparedNumber%2 == 0;
            boolean isEvenToCompare = toCompare%2 == 0;
            if (isEvenCompared && !isEvenToCompare) {
                return 1;
            } else if (!isEvenCompared) {
                return -1;
            } else {
                return comparedNumber > toCompare ? 1 : -1;
            }
        }
    },
    LOWER_AND_EVEN {
        @Override
        public int compare(Integer comparedNumber, Integer toCompare) {
            boolean isEvenCompared = comparedNumber%2 == 0;
            boolean isEvenToCompare = toCompare%2 == 0;
            if (isEvenCompared && !isEvenToCompare) {
                return -1;
            } else if (!isEvenCompared) {
                return 1;
            } else {
                return comparedNumber > toCompare ? 1 : -1;
            }
        }
    }
}

enum Filters implements SerializableFunction<String, Boolean> {
    NOT_EMPTY {
        @Override
        public Boolean apply(String input) {
            return !input.isEmpty();
        }
    }
}