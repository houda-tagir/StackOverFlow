package com.sightstack.dto;

import com.sightstack.model.Question;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchResponse {
    private List<Question> results;
    private int totalResults;
    private long searchTimeMs;
}