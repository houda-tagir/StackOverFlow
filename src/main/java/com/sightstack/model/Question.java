package com.sightstack.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Question {
    private String id;
    private String title;
    private String body;
    private LocalDateTime creationDate;
    private int score;
    private int ownerReputation;
    private List<Answer> answers;
    private List<String> tags;
}