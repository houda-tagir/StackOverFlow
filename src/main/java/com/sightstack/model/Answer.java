package com.sightstack.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Answer {
    private String id;
    private String body;
    private int score;
    private boolean isAccepted;
    private int ownerReputation;
}