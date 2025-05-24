package com.sightstack.controller;

import com.sightstack.dto.SearchRequest;
import com.sightstack.dto.SearchResponse;
import com.sightstack.service.QuestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/search")
@RequiredArgsConstructor
@Slf4j
public class SearchController {

    private final QuestionService questionService;

    @GetMapping
    public ResponseEntity<SearchResponse> search(
            @RequestParam(name = "q", required = false) String query,
            @RequestParam(name = "tags", required = false) String tagList,
            @RequestParam(name = "limit", defaultValue = "10") int limit) {

        log.info("Search request: query={}, tags={}, limit={}", query, tagList, limit);

        // Parse tags
        List<String> tags = tagList != null && !tagList.isEmpty() ?
                Arrays.stream(tagList.split(",")).collect(Collectors.toList()) :
                null;

        // Create search request
        SearchRequest request = SearchRequest.builder()
                .query(query)
                .tags(tags)
                .limit(limit)
                .build();

        // Execute search
        SearchResponse response = questionService.searchQuestions(request);

        return ResponseEntity.ok(response);
    }

    @GetMapping("/suggest")
    public ResponseEntity<List<String>> suggest(@RequestParam("prefix") String prefix) {
        log.info("Suggest request: prefix={}", prefix);

        if (prefix == null || prefix.length() < 2) {
            return ResponseEntity.ok(List.of());
        }

        List<String> suggestions = questionService.suggestQuestionTitles(prefix);
        return ResponseEntity.ok(suggestions);
    }
}