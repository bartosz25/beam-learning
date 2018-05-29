package com.waitingforcode.pcollection;

import com.waitingforcode.BeamFunctions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.waitingforcode.pcollection.TextAccumulators.LETTERS;
import static com.waitingforcode.pcollection.TextAccumulators.NOT_SERIALIZABLE_PEOPLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnit4.class)
public class CoderTest implements Serializable {

    @Test
    public void should_fail_when_not_deterministic_coder_is_defined_for_group_by_key_transform() {
        Pipeline pipeline = BeamFunctions.createPipeline("Not deterministic coder for group by key transform");
        CoderProvider stringCustomProvider = CoderProviders.fromStaticMethods(String.class,
                CustomStringNonDeterministicCoder.class);
        pipeline.getCoderRegistry().registerCoderProvider(stringCustomProvider);
        PCollection<KV<String, Integer>> lettersCollection = pipeline.apply(Create.of(
                KV.of("a", 1), KV.of("a", 1), KV.of("b", 2)));

        assertThatThrownBy(() -> {
            lettersCollection.apply(GroupByKey.create()).apply(Count.perKey());
            pipeline.run().waitUntilFinish();
        }).isInstanceOf(IllegalStateException.class)
                .hasMessage("the keyCoder of a GroupByKey must be deterministic");
    }

    @Test
    public void should_override_default_coder_for_strings() throws ClassNotFoundException {
        Pipeline pipeline = BeamFunctions.createPipeline("String coder overridden");
        CoderProvider stringCustomProvider = CoderProviders.fromStaticMethods(String.class, CustomStringCoder.class);
        pipeline.getCoderRegistry().registerCoderProvider(stringCustomProvider);
        pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));

        pipeline.run().waitUntilFinish();

        List<String> encodedWords = LETTERS.getTexts();
        System.out.println("texts were="+encodedWords);
        assertThat(encodedWords).contains("a", "b", "c", "a", "b", "a", "b", "c", "c");
    }

    @Test
    public void should_use_correct_coder_for_java_bean() throws CannotProvideCoderException {
        Pipeline pipeline = BeamFunctions.createPipeline("Java bean coder");
        PCollection<Person> people = pipeline.apply(Create.of(Arrays.asList(Person.of("a", "1"), Person.of("b", "2"), Person.of("c", "3"))));

        Coder<Person> personCoder = people.getCoder();

        pipeline.run().waitUntilFinish();

        assertThat(personCoder).isInstanceOf(SerializableCoder.class);
    }

    @Test
    public void should_fail_when_no_default_coder_can_be_found_for_java_bean() throws CannotProvideCoderException {
        Pipeline pipeline = BeamFunctions.createPipeline("Java bean PCollection without default coder");

        assertThatThrownBy(() ->  {
            pipeline.apply(Create.of(Arrays.asList(new PersonNoSerializable())));
            pipeline.run().waitUntilFinish();
        }).isInstanceOf(IllegalArgumentException.class).hasMessage("Unable to infer a coder and no Coder was specified. " +
                "Please set a coder by invoking Create.withCoder() explicitly.");
    }

    @Test
    public void should_correctly_handle_collection() throws CannotProvideCoderException {
        Pipeline pipeline = BeamFunctions.createPipeline("Collection coder");
        PCollection<List<Integer>> numbers = pipeline.apply(Create.of(Arrays.asList(Arrays.asList(1, 2, 3),
                Arrays.asList(3, 4, 5))));
        Coder<List<Integer>> listCoder = numbers.getCoder();

        pipeline.run().waitUntilFinish();

        assertThat(listCoder).isInstanceOf(ListCoder.class);
        assertThat(listCoder.getCoderArguments().get(0)).isInstanceOf(VarIntCoder.class);
    }

    @Test
    public void should_read_java_bean_with_custom_coder() {
        Pipeline pipeline = BeamFunctions.createPipeline("Custom Java bean coder");
        PCollection<PersonNoSerializable> peopleDataset = pipeline
                .apply(Create.of(Arrays.asList(PersonNoSerializable.of("fname2", "lname2")))
                        .withCoder(new CustomPersonNoSerializableCoder()));
        peopleDataset.apply(ParDo.of(new DoFn<PersonNoSerializable, String>() {
            @ProcessElement
            public void process(ProcessContext processContext) {
                String decodedPerson = processContext.element().getFirstName() + "_" + processContext.element().getLastName();
                NOT_SERIALIZABLE_PEOPLE.addText(decodedPerson);
                processContext.output(decodedPerson);
            }
        }));

        pipeline.run().waitUntilFinish();

        assertThat(NOT_SERIALIZABLE_PEOPLE.getTexts()).hasSize(1)
                .containsOnly("fname2-write-read-write-read_lname2-write-read-write-read");
    }

    public static class CustomStringNonDeterministicCoder extends AtomicCoder<String> {

        public static CustomStringNonDeterministicCoder of() {
            return new CustomStringNonDeterministicCoder();
        }

        @Override
        public void encode(String text, OutputStream outStream) throws CoderException, IOException {
            byte[] textBytes = text.getBytes();
            outStream.write(textBytes);
        }

        @Override
        public String decode(InputStream inStream) throws IOException {
            return "";
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            throw new NonDeterministicException(this, "Custom string coder is not deterministic");
        }
    }

    public static class CustomStringCoder extends AtomicCoder<String> {

        public static CustomStringCoder of() {
            return new CustomStringCoder();
        }

        @Override
        public void encode(String text, OutputStream outStream) throws IOException {
            byte[] textBytes = text.getBytes();
            LETTERS.addText(text);
            outStream.write(textBytes);
        }

        @Override
        public String decode(InputStream inStream) throws IOException {
            byte[] bytes = StreamUtils.getBytes(inStream);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    public static class Person implements Serializable {
        private String firstName;

        private String lastName;


        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public static Person of(String firstName, String lastName) {
            Person person = new Person();
            person.firstName = firstName;
            person.lastName = lastName;
            return person;
        }
    }

    public static class PersonNoSerializable {
        private String firstName;
        private String lastName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public static PersonNoSerializable of(String firstName, String lastName) {
            PersonNoSerializable person = new PersonNoSerializable();
            person.firstName = firstName;
            person.lastName = lastName;
            return person;
        }
    }

    public static class CustomPersonNoSerializableCoder extends Coder<PersonNoSerializable> {

        private static final String NAMES_SEPARATOR = "_";

        @Override
        public void encode(PersonNoSerializable person, OutputStream outStream) throws IOException {
            // Dummy encoder, separates first and last names by _
            String serializablePerson = person.getFirstName()+"-write"+NAMES_SEPARATOR+person.getLastName()+"-write";
            outStream.write(serializablePerson.getBytes());
        }

        @Override
        public PersonNoSerializable decode(InputStream inStream) throws CoderException, IOException {
            String serializedPerson = new String(StreamUtils.getBytes(inStream));
            String[] names = serializedPerson.split(NAMES_SEPARATOR);
            return PersonNoSerializable.of(names[0]+"-read", names[1]+"-read");
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}
    }



    // TODO : custom coder
    // TODO : coder for Java bean
    // TODO : coder for collection
}



enum TextAccumulators {
    LETTERS, NOT_SERIALIZABLE_PEOPLE;

    private List<String> texts = new ArrayList<>();

    public void addText(String text) {
        texts.add(text);
    }

    public List<String> getTexts() {
        return texts;
    }
}