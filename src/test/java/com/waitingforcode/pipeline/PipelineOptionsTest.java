package com.waitingforcode.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnit4.class)
public class PipelineOptionsTest {

    @Test
    public void should_fail_when_the_job_name_is_not_unique_and_stableUniqueNames_is_set_to_error() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("stableUniqueNames set to ERROR");
        options.setStableUniqueNames(PipelineOptions.CheckEnabled.ERROR);
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> lettersCollection = pipeline.apply(Create.of(Arrays.asList("a", "b", "c", "d")));

        lettersCollection.apply("Counter", Count.globally());
        lettersCollection.apply("Counter", Count.globally());

        assertThatThrownBy(() -> pipeline.run().waitUntilFinish()).isInstanceOf(IllegalStateException.class)
                .hasMessage("Pipeline update will not be possible because the following transforms " +
                        "do not have stable unique names: Counter2.");
    }

    @Test
    public void should_not_fail_on_setting_job_name_not_respecting_naming_rules_for_directrunner() {
        PipelineOptions options = PipelineOptionsFactory.create();
        // Only DataflowRunner is constrained by the naming conventions:
        // See org.apache.beam.runners.dataflow.DataflowRunner.fromOptions() where the RegEx validation is defined as
        // jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?")
        options.setJobName("<>..///!...:;:;é'à''çà)");
        // DirectRunner is the default runner so there is no need to define it
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> lettersCollection = pipeline.apply(Create.of(Arrays.asList("a", "b", "c", "d")));

        lettersCollection.apply("Counter", Count.globally());

        pipeline.run().waitUntilFinish();
    }

}
