package com.waitingforcode.pcollection;

import com.google.common.collect.Iterables;
import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JoinTest implements Serializable {

    @Test
    public void should_join_2_datasets_which_all_have_1_matching_key_with_native_sdk_code() {
        Pipeline pipeline = BeamFunctions.createPipeline("1:n join with native SDK");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110),
                KV.of("user1", 1200), KV.of("user2", 220), KV.of("user3", 120)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));

        final TupleTag<Integer> amountTagDataset1 = new TupleTag<>();
        final TupleTag<Integer> amountTagDataset2 = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
                .of(amountTagDataset1, ordersPerUser1Dataset)
                .and(amountTagDataset2, ordersPerUser2Dataset)
                .apply(CoGroupByKey.create());

        PCollection<KV<String, Integer>> totalAmountsPerUser = groupedCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KV<String, CoGbkResult> element = processContext.element();
                Iterable<Integer> dataset1Amounts = element.getValue().getAll(amountTagDataset1);
                Iterable<Integer> dataset2Amounts = element.getValue().getAll(amountTagDataset2);
                Integer sumAmount = StreamSupport.stream(Iterables.concat(dataset1Amounts, dataset2Amounts).spliterator(), false)
                        .collect(Collectors.summingInt(n -> n));
                processContext.output(KV.of(element.getKey(), sumAmount));
            }
        }));

        PAssert.that(totalAmountsPerUser).containsInAnyOrder(KV.of("user1", 3300), KV.of("user2", 630),
                KV.of("user3", 330));
        pipeline.run().waitUntilFinish();
    }


    @Test

    public void should_join_2_datasets_with_side_inputs() {
        Pipeline pipeline = BeamFunctions.createPipeline("Broadcast join with side input");
        List<KV<String, String>> ordersWithCountry = Arrays.asList(
                KV.of("order_1", "fr"), KV.of("order_2", "fr"), KV.of("order_3", "pl")
        );
        List<KV<String, String>> countriesWithIsoCode = Arrays.asList(
                KV.of("fr", "France"), KV.of("pl", "Poland"), KV.of("de", "Germany")
        );

        PCollection<KV<String, String>> ordersWithCountriesDataset = pipeline.apply(Create.of(ordersWithCountry));
        PCollection<KV<String, String>> countriesMapDataset = pipeline.apply(Create.of(countriesWithIsoCode));
        PCollectionView<Map<String, String>> countriesSideInput = countriesMapDataset.apply(View.asMap());
        PCollection<String> ordersSummaries = ordersWithCountriesDataset.apply(ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                Map<String, String> countriesByIso = context.sideInput(countriesSideInput);
                KV<String, String> processedElement = context.element();
                String orderCountry = countriesByIso.get(processedElement.getValue());
                String orderSummary = processedElement.getKey() + " (" + orderCountry + ")";
                context.output(orderSummary);
            }
        }).withSideInputs(countriesSideInput));

        PAssert.that(ordersSummaries).containsInAnyOrder("order_1 (France)", "order_2 (France)", "order_3 (Poland)");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_do_inner_join_on_2_datasets_with_sdk_extension() {
        Pipeline pipeline = BeamFunctions.createPipeline("Inner join with the extension");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100), KV.of("user6", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110), KV.of("user7", 100)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));

        PCollection<KV<String, KV<Integer, Integer>>> joinedDatasets = Join.innerJoin(ordersPerUser1Dataset, ordersPerUser2Dataset);
        PCollection<KV<String, Integer>> amountsPerUser = joinedDatasets.apply(ParDo.of(new AmountsCalculator()));

        // user6 and user7 are ignored because they're not included in both datasets
        PAssert.that(amountsPerUser).containsInAnyOrder(KV.of("user1", 2100), KV.of("user2", 410), KV.of("user3", 210));
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void should_output_output_2_pairs_for_1_to_2_relationship_join() {
        Pipeline pipeline = BeamFunctions.createPipeline("Extension inner join for 1:n relationship");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110), KV.of("user2", 300)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));
        PCollection<KV<String, KV<Integer, Integer>>> joinedDatasets = Join.innerJoin(ordersPerUser1Dataset, ordersPerUser2Dataset);
        PCollection<KV<String, Integer>> amountsPerUser = joinedDatasets.apply(ParDo.of(new AmountsCalculator()));

        // Join extension gives a little bit less of flexibility than the custom join processing for the case of 1:n
        // joins. It doesn't allow to combine multiple values into a single output. Instead it returns every
        // combination of joined keys
        PAssert.that(amountsPerUser).containsInAnyOrder(KV.of("user1", 2100), KV.of("user2", 410), KV.of("user3", 210),
                KV.of("user2", 500));
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void should_do_outer_full_join_on_2_datasets_with_sdk_extension() {
        Pipeline pipeline = BeamFunctions.createPipeline("Extension outer full join");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100), KV.of("user6", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110), KV.of("user7", 100)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));

        PCollection<KV<String, KV<Integer, Integer>>> joinedDatasets = Join.fullOuterJoin(ordersPerUser1Dataset, ordersPerUser2Dataset,
                0, 0);
        PCollection<KV<String, Integer>> amountsPerUser = joinedDatasets.apply(ParDo.of(new AmountsCalculator()));
        // user6 and user7 are ignored because they're not included in both datasets
        PAssert.that(amountsPerUser).containsInAnyOrder(KV.of("user1", 2100), KV.of("user2", 410), KV.of("user3", 210),
                KV.of("user6", 100), KV.of("user7", 100));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_do_outer_left_join_on_2_datasets_with_sdk_extension() {
        Pipeline pipeline = BeamFunctions.createPipeline("Extension outer left join");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100), KV.of("user6", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110), KV.of("user7", 100)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));
        PCollection<KV<String, KV<Integer, Integer>>> joinedDatasets = Join.leftOuterJoin(ordersPerUser1Dataset, ordersPerUser2Dataset,
                0);
        PCollection<KV<String, Integer>> amountsPerUser = joinedDatasets.apply(ParDo.of(new AmountsCalculator()));
        // user6 and user7 are ignored because they're not included in both datasets
        PAssert.that(amountsPerUser).containsInAnyOrder(KV.of("user1", 2100), KV.of("user2", 410), KV.of("user3", 210),
                KV.of("user6", 100));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void should_do_outer_right_join_on_2_datasets_with_sdk_extension() {
        Pipeline pipeline = BeamFunctions.createPipeline("Extension outer right join");
        List<KV<String, Integer>> ordersPerUser1 = Arrays.asList(
                KV.of("user1", 1000), KV.of("user2", 200), KV.of("user3", 100), KV.of("user6", 100)
        );
        List<KV<String, Integer>> ordersPerUser2 = Arrays.asList(
                KV.of("user1", 1100), KV.of("user2", 210), KV.of("user3", 110), KV.of("user7", 100)
        );

        PCollection<KV<String, Integer>> ordersPerUser1Dataset = pipeline.apply(Create.of(ordersPerUser1));
        PCollection<KV<String, Integer>> ordersPerUser2Dataset = pipeline.apply(Create.of(ordersPerUser2));
        PCollection<KV<String, KV<Integer, Integer>>> joinedDatasets = Join.rightOuterJoin(ordersPerUser1Dataset, ordersPerUser2Dataset,
                0);
        PCollection<KV<String, Integer>> amountsPerUser = joinedDatasets.apply(ParDo.of(new AmountsCalculator()));
        // user6 and user7 are ignored because they're not included in both datasets
        PAssert.that(amountsPerUser).containsInAnyOrder(KV.of("user1", 2100), KV.of("user2", 410), KV.of("user3", 210),
                KV.of("user7", 100));
        pipeline.run().waitUntilFinish();
    }

    class AmountsCalculator extends DoFn<KV<String, KV<Integer, Integer>>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext processContext) {
            KV<String, KV<Integer, Integer>> element = processContext.element();
            int totalAmount = element.getValue().getKey() + element.getValue().getValue();
            processContext.output(KV.of(element.getKey(), totalAmount));
        }
    }

}
