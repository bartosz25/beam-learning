package com.waitingforcode.pcollection;

import org.apache.beam.sdk.transforms.SerializableFunction;

public abstract class Transformations {

    public enum Filters implements SerializableFunction<String, Boolean> {
        NOT_EMPTY {
            @Override
            public Boolean apply(String input) {
                return !input.isEmpty();
            }
        }
    }

    public enum IntMapper implements SerializableFunction<String, Integer> {
        FROM_TEXT_LENGTH {
            @Override
            public Integer apply(String input) {
                return input.length();
            }
        }
    }

}
