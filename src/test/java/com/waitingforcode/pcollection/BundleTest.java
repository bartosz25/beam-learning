package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class BundleTest implements Serializable {

    private static final String BASE_DIR = "/tmp/beam/bundles/";
    private static final String FILE_1 = BASE_DIR+"1";
    private static final String FILE_2 = BASE_DIR+"2";

    @BeforeClass
    public static void writeFiles() throws IOException {
        String linesFile1 = IntStream.rangeClosed(1, 200).boxed().map(number -> number.toString())
                .collect(Collectors.joining("\n"));
        FileUtils.writeStringToFile(new File(FILE_1), linesFile1, "UTF-8");
        String linesFile2 = IntStream.rangeClosed(201, 400).boxed().map(number -> number.toString())
                .collect(Collectors.joining("\n"));
        FileUtils.writeStringToFile(new File(FILE_2), linesFile2, "UTF-8");
    }

    @AfterClass
    public static void deleteFiles() {
        FileUtils.deleteQuietly(new File(FILE_1));
        FileUtils.deleteQuietly(new File(FILE_2));
    }

    @Test
    public void should_show_how_bundle_is_divided_through_pardo() {
        Pipeline pipeline = BeamFunctions.createPipeline("Illustrating bundle division for 1 file", 20);
        PCollection<String> file1Content = pipeline.apply(TextIO.read().from(FILE_1));

        file1Content.apply(ParDo.of(new BundlesDetector(Containers.TEST_1_FILE)));

        pipeline.run().waitUntilFinish();
        Map<String, List<String>> bundlesData = Containers.TEST_1_FILE.getBundlesData();
        assertThat(bundlesData.size()).isGreaterThan(1);
        Set<String> dataInBundles = new HashSet<>();
        bundlesData.forEach((bundleKey, data) -> dataInBundles.addAll(data));
        assertThat(dataInBundles).hasSize(200);
        IntStream.rangeClosed(1, 200).boxed().map(number -> number.toString())
                .forEach(letter -> assertThat(dataInBundles).contains(letter));
        System.out.println("========== Debug 1 file =========");
        bundlesData.forEach((bundleKey, data) -> printDebugMessage(bundleKey, data));
    }

    @Test
    public void should_define_more_bundles_with_greater_level_of_parallelism() {
        Pipeline pipelineParallelism2 = BeamFunctions.createPipeline("Illustrating bundle division for 1 file for " +
                "parallelism of 2", 2);
        Pipeline pipelineParallelism20 = BeamFunctions.createPipeline("Illustrating bundle division for 1 file for " +
                "parallelism of 20", 20);
        PCollection<String> file1ContentPar2 = pipelineParallelism2.apply(TextIO.read().from(FILE_1));
        PCollection<String> file1ContentPar20 = pipelineParallelism20.apply(TextIO.read().from(FILE_1));

        file1ContentPar2.apply(ParDo.of(new BundlesDetector(Containers.TEST_1_FILE_PAR_2)));
        file1ContentPar20.apply(ParDo.of(new BundlesDetector(Containers.TEST_1_FILE_PAR_20)));
        pipelineParallelism2.run().waitUntilFinish();
        pipelineParallelism20.run().waitUntilFinish();

        Map<String, List<String>> bundlesDataPar2 = Containers.TEST_1_FILE_PAR_2.getBundlesData();
        Set<Integer> itemsInBundlesPar2 = new HashSet<>();
        bundlesDataPar2.values().forEach(bundleItems -> itemsInBundlesPar2.add(bundleItems.size()));
        // There are much more numbers but they're not always the same. The numbers below appeared in every
        // tested division
        // As you can see, the bundle's division is much less even as in the case of parallelism of 20
        assertThat(itemsInBundlesPar2).contains(1, 2, 4, 5, 7, 10, 34, 42);
        System.out.println("========== Debug 1 file - par 2 =========");
        bundlesDataPar2.forEach((bundleKey, data) -> printDebugMessage(bundleKey, data));

        Map<String, List<String>> bundlesDataPar20 = Containers.TEST_1_FILE_PAR_20.getBundlesData();
        Set<Integer> itemsInBundlesPar20 = new HashSet<>();
        bundlesDataPar20.values().forEach(bundleItems -> {
            itemsInBundlesPar20.add(bundleItems.size());
        });
        assertThat(itemsInBundlesPar20).contains(1, 3, 8, 9, 10, 11, 12, 15);
        System.out.println("========== Debug 1 file - par 20 =========");
        bundlesDataPar20.forEach((bundleKey, data) -> printDebugMessage(bundleKey, data));
    }

    @Test
    public void should_process_data_in_bundles_even_for_2_read_files() {
        Pipeline pipeline = BeamFunctions.createPipeline("Illustrating bundle division for 2 files");
        PCollection<String> file1Content = pipeline.apply(TextIO.read().from(BASE_DIR+"/*"));

        file1Content.apply(ParDo.of(new BundlesDetector(Containers.TEST_2_FILES)));

        pipeline.run().waitUntilFinish();
        Map<String, List<String>> bundlesData = Containers.TEST_2_FILES.getBundlesData();
        assertThat(bundlesData.size()).isGreaterThan(1);
        Set<String> dataInBundles = new HashSet<>();
        bundlesData.forEach((bundleKey, data) -> dataInBundles.addAll(data));
        assertThat(dataInBundles).hasSize(400);
        IntStream.rangeClosed(1, 400).boxed().map(number -> number.toString())
                .forEach(letter -> assertThat(dataInBundles).contains(letter));
        System.out.println("========== Debug 2 files =========");
        bundlesData.forEach((bundleKey, data) -> printDebugMessage(bundleKey, data));
    }

    private static void printDebugMessage(String bundleKey, Collection<String> bundleData) {
        System.out.println("["+bundleKey+"]");
        System.out.println("=> "+bundleData);
    }

}

class BundlesDetector extends DoFn<String, String> {

    private String bundleName;

    private Containers container;

    public BundlesDetector(Containers container) {
        this.container = container;
    }

    @DoFn.StartBundle
    public void initializeBundle() {
        this.bundleName = UUID.randomUUID().toString();
    }

    @DoFn.ProcessElement
    public void processLine(ProcessContext processContext) {
        container.addItem(bundleName, processContext.element());
    }
}

enum Containers {

    TEST_1_FILE, TEST_1_FILE_PAR_2, TEST_1_FILE_PAR_20, TEST_2_FILES;

    private Map<String, List<String>> bundlesData = new ConcurrentHashMap<>();

    public void addItem(String bundle, String item) {
        List<String> accumulatedItems = bundlesData.getOrDefault(bundle, new ArrayList<>());
        accumulatedItems.add(item);
        bundlesData.put(bundle, accumulatedItems);
    }

    public Map<String, List<String>> getBundlesData() {
        return bundlesData;
    }

}
