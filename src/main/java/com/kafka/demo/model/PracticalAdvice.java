package com.kafka.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PracticalAdvice(@JsonProperty String message,
                              @JsonProperty int identifier) {
}
