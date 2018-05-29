package com.waitingforcode.pipeline;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class PipelineTest implements Serializable {

    @Test
    public void should_not_execute_pipeline_when_runner_is_not_called() {
        String fileName = "/tmp/beam/file_no_runner";
        Pipeline pipeline = BeamFunctions.createPipeline("No file generated");
        PCollection<String> inputNumbers = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));

        inputNumbers.apply(TextIO.write().to(fileName));

        File writtenFile = new File(fileName);
        assertThat(writtenFile).doesNotExist();
    }


    @Test
    public void should_show_composite_transform_wrapping_primitive_ones() {
        Pipeline pipeline = BeamFunctions.createPipeline("Composite transform");
        List<Integer> numbers = Arrays.asList(1, 100, 200, 201, 202, 203, 330, 400, 500);
        PCollection<Integer> inputNumbers = pipeline.apply("create", Create.of(numbers));
        class MathOperator extends PTransform<PCollection<Integer>, PCollection<Integer>> {

            private int minValue;
            private int multiplier;
            private int divisor;

            public MathOperator(int minValue, int multiplier, int divisor) {
                this.minValue = minValue;
                this.multiplier = multiplier;
                this.divisor = divisor;
            }

            @Override
            public PCollection<Integer> expand(PCollection<Integer> inputNumbers) {
                return inputNumbers.apply("gt filter", Filter.greaterThan(minValue))
                        .apply("multiplier", MapElements.into(TypeDescriptors.integers()).via(number -> number*multiplier))
                        .apply("divisor", MapElements.into(TypeDescriptors.integers()).via(number -> number/divisor));
            }
        }
        inputNumbers.apply("composite operation", new MathOperator(200, 5, 2));
        NodesVisitor visitor = new NodesVisitor();

        pipeline.traverseTopologically(visitor);

        pipeline.run().waitUntilFinish();
        List<TransformHierarchy.Node> visitedNodes = visitor.getVisitedNodes();
        String graph = visitor.stringifyVisitedNodes();
        assertThat(graph).isEqualTo("[ROOT]  -> create[composite](out: create/Read(CreateSource).out) -> " +
                "create/Read(CreateSource)(out: create/Read(CreateSource).out) ->  " +
                "(in:  create/Read(CreateSource).out) composite operation[composite]" +
                    "(out: composite operation/divisor/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  create/Read(CreateSource).out) composite operation/gt filter[composite]" +
                    "(out: composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  create/Read(CreateSource).out) composite operation/gt filter/ParDo(Anonymous)[composite]" +
                    "(out: composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  create/Read(CreateSource).out) composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous)" +
                    "(out: composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) " +
                    "composite operation/multiplier[composite]" +
                    "(out: composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) " +
                    "composite operation/multiplier/Map[composite]" +
                    "(out: composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/gt filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) " +
                    "composite operation/multiplier/Map/ParMultiDo(Anonymous)" +
                    "(out: composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) " +
                    "composite operation/divisor[composite]" +
                    "(out: composite operation/divisor/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) " +
                    "composite operation/divisor/Map[composite]" +
                    "(out: composite operation/divisor/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  composite operation/multiplier/Map/ParMultiDo(Anonymous).out0) " +
                    "composite operation/divisor/Map/ParMultiDo(Anonymous)" +
                    "(out: composite operation/divisor/Map/ParMultiDo(Anonymous).out0)");
    }

    @Test
    public void should_show_pipeline_with_filter_and_2_transforms() {
        Pipeline pipeline = BeamFunctions.createPipeline("Filter and 2 transforms");
        List<Integer> numbers = Arrays.asList(1, 100, 200, 201, 202, 203, 330, 400, 500);
        PCollection<Integer> inputNumbers = pipeline.apply("create", Create.of(numbers));
        // every almost native transform is a composite, e.g. filter implements expand method (BTW it's the contract
        // since every PTransform implementation must implement this method)
        PCollection<Integer> filteredNumbers = inputNumbers.apply("filter", Filter.greaterThan(200));
        PCollection<Integer> multipliedNumbers = filteredNumbers
                .apply("map1", MapElements.into(TypeDescriptors.integers()).via(number -> number * 5))
                .apply("map2", MapElements.into(TypeDescriptors.integers()).via(number -> number / 2));
        NodesVisitor visitor = new NodesVisitor();

        pipeline.traverseTopologically(visitor);

        pipeline.run().waitUntilFinish();
        String graph = visitor.stringifyVisitedNodes();
        assertThat(graph).isEqualTo("[ROOT]  -> create[composite](out: create/Read(CreateSource).out) -> " +
                "create/Read(CreateSource)(out: create/Read(CreateSource).out) ->  " +
                "(in:  create/Read(CreateSource).out) filter[composite]" +
                    "(out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  create/Read(CreateSource).out) filter/ParDo(Anonymous)[composite]" +
                    "(out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  create/Read(CreateSource).out) filter/ParDo(Anonymous)/ParMultiDo(Anonymous)" +
                    "(out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) map1[composite]" +
                    "(out: map1/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) map1/Map[composite]" +
                    "(out: map1/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) map1/Map/ParMultiDo(Anonymous)" +
                    "(out: map1/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  map1/Map/ParMultiDo(Anonymous).out0) map2[composite]" +
                    "(out: map2/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  map1/Map/ParMultiDo(Anonymous).out0) map2/Map[composite]" +
                    "(out: map2/Map/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  map1/Map/ParMultiDo(Anonymous).out0) map2/Map/ParMultiDo(Anonymous)" +
                    "(out: map2/Map/ParMultiDo(Anonymous).out0)");
    }

    @Test
    public void should_show_pipeline_with_2_root_nodes() {
        Pipeline pipeline = BeamFunctions.createPipeline("2 root nodes");
        List<Integer> numbers = Arrays.asList(1, 100, 200, 201, 202, 203, 330, 400, 500);
        PCollection<Integer> rootNode1 = pipeline.apply("number1", Create.of(numbers));
        PCollection<Integer> rootNode2 = pipeline.apply("numbers2", Create.of(numbers));
        PCollection<Integer> filteredNumbers1 = rootNode1.apply("filter1", Filter.greaterThan(200));
        PCollection<Integer> filteredNumbers2 = rootNode2.apply("filter2", Filter.greaterThan(200));
        NodesVisitor visitor = new NodesVisitor();

        pipeline.traverseTopologically(visitor);

        pipeline.run().waitUntilFinish();
        String graph = visitor.stringifyVisitedNodes();
        assertThat(graph).isEqualTo("[ROOT]  -> number1[composite](out: number1/Read(CreateSource).out) -> " +
                "number1/Read(CreateSource)(out: number1/Read(CreateSource).out) -> " +
                "numbers2[composite](out: numbers2/Read(CreateSource).out) -> numbers2/Read(CreateSource)" +
                    "(out: numbers2/Read(CreateSource).out) ->  " +
                "(in:  number1/Read(CreateSource).out) filter1[composite]" +
                    "(out: filter1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  number1/Read(CreateSource).out) filter1/ParDo(Anonymous)[composite]" +
                    "(out: filter1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  number1/Read(CreateSource).out) filter1/ParDo(Anonymous)/ParMultiDo(Anonymous)" +
                    "(out: filter1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  numbers2/Read(CreateSource).out) filter2[composite]" +
                    "(out: filter2/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  numbers2/Read(CreateSource).out) filter2/ParDo(Anonymous)[composite]" +
                    "(out: filter2/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  numbers2/Read(CreateSource).out) filter2/ParDo(Anonymous)/ParMultiDo(Anonymous)" +
                    "(out: filter2/ParDo(Anonymous)/ParMultiDo(Anonymous).out0)");
    }

    @Test
    public void should_show_pipeline_with_simple_filter_transform() {
        Pipeline pipeline = BeamFunctions.createPipeline("Filter transform");
        PCollection<Integer> inputNumbers = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
        inputNumbers.apply("filter_1", Filter.greaterThanEq(2));
        NodesVisitor visitor = new NodesVisitor();

        pipeline.traverseTopologically(visitor);

        pipeline.run().waitUntilFinish();
        String graph = visitor.stringifyVisitedNodes();
        assertThat(graph).isEqualTo("[ROOT]  -> Create.Values[composite](out: Create.Values/Read(CreateSource).out) -> " +
                "Create.Values/Read(CreateSource)(out: Create.Values/Read(CreateSource).out) ->  " +
                "(in:  Create.Values/Read(CreateSource).out) filter_1[composite]" +
                    "(out: filter_1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  Create.Values/Read(CreateSource).out) filter_1/ParDo(Anonymous)[composite]" +
                    "(out: filter_1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0) ->  " +
                "(in:  Create.Values/Read(CreateSource).out) filter_1/ParDo(Anonymous)/ParMultiDo(Anonymous)" +
                    "(out: filter_1/ParDo(Anonymous)/ParMultiDo(Anonymous).out0)");
    }

}

class NodesVisitor implements Pipeline.PipelineVisitor {

    private List<TransformHierarchy.Node> visitedNodes = new ArrayList<>();

    public List<TransformHierarchy.Node> getVisitedNodes() {
        return visitedNodes;
    }

    @Override
    public void enterPipeline(Pipeline p) {
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        visitedNodes.add(node);
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
        visitedNodes.add(node);
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
    }

    @Override
    public void leavePipeline(Pipeline pipeline) {

    }

    public String stringifyVisitedNodes() {
        return  visitedNodes.stream().map(node -> {
            if (node.isRootNode()) {
                return "[ROOT] ";
            }
            String compositeFlagStringified = node.isCompositeNode() ? "[composite]" : "";
            String inputs = stringifyValues(node.getInputs());
            String inputsStringified = inputs.isEmpty() ? "" : " (in:  " + inputs + ") ";
            String outputs = stringifyValues(node.getOutputs());
            String outputsStringified = outputs.isEmpty() ? "" : "(out: " + outputs + ")";
            return inputsStringified + node.getFullName() + compositeFlagStringified + outputsStringified;
        }).collect(Collectors.joining(" -> "));
    }

    private static String stringifyValues(Map<TupleTag<?>, PValue> values) {
        return values.entrySet().stream()
                .map(entry -> entry.getValue().getName()).collect(Collectors.joining(","));
    }
}