package com.sightstack.service;

import com.sightstack.dto.SearchRequest;
import com.sightstack.dto.SearchResponse;
import com.sightstack.dto.TrendData;
import com.sightstack.model.Answer;
import com.sightstack.model.Question;
import com.sightstack.repository.HBaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class QuestionService {

    private final HBaseRepository hBaseRepository;

    @Value("${search.api.max-results}")
    private int maxSearchResults;

    @Value("${search.api.autosuggest.max-suggestions}")
    private int maxSuggestions;

    public SearchResponse searchQuestions(SearchRequest request) {
        long startTime = System.currentTimeMillis();

        int limit = request.getLimit() > 0 ? Math.min(request.getLimit(), maxSearchResults) : maxSearchResults;
        List<Question> questions = hBaseRepository.searchQuestions(request.getQuery(), request.getTags(), limit);

        // Process each question to select top-3 answers
        questions.forEach(this::selectTopAnswers);

        long endTime = System.currentTimeMillis();
        return SearchResponse.builder()
                .results(questions)
                .totalResults(questions.size())
                .searchTimeMs(endTime - startTime)
                .build();
    }

    public List<String> suggestQuestionTitles(String prefix) {
        return hBaseRepository.suggestQuestionTitles(prefix, maxSuggestions);
    }

    public TrendData getTrendData(String tag, String period) {
        int pointCount = getPeriodPointCount(period);
        List<Integer> data = hBaseRepository.getTrendData(tag, period, pointCount);

        // Convert to time series points
        List<TrendData.TimeSeriesPoint> timeSeriesData = IntStream.range(0, data.size())
                .mapToObj(i -> {
                    LocalDateTime timestamp = getTimestampForIndex(period, i);
                    return TrendData.TimeSeriesPoint.builder()
                            .timestamp(timestamp)
                            .count(data.get(i))
                            .build();
                })
                .collect(Collectors.toList());

        return TrendData.builder()
                .tag(tag)
                .period(period)
                .data(timeSeriesData)
                .build();
    }

    private int getPeriodPointCount(String period) {
        switch (period.toLowerCase()) {
            case "hour":
                return 60; // One point per minute
            case "day":
                return 24; // One point per hour
            case "month":
                return 30; // One point per day
            default:
                return 24;
        }
    }

    private LocalDateTime getTimestampForIndex(String period, int index) {
        LocalDateTime now = LocalDateTime.now();

        switch (period.toLowerCase()) {
            case "hour":
                return now.minusMinutes(59 - index);
            case "day":
                return now.minusHours(23 - index);
            case "month":
                return now.minusDays(29 - index);
            default:
                return now.minusHours(23 - index);
        }
    }

    private void selectTopAnswers(Question question) {
        List<Answer> answers = question.getAnswers();
        if (answers == null || answers.isEmpty()) {
            question.setAnswers(new ArrayList<>());
            return;
        }

        // Step 1: Find accepted answer (if any)
        List<Answer> selectedAnswers = new ArrayList<>();
        answers.stream()
                .filter(Answer::isAccepted)
                .findFirst()
                .ifPresent(selectedAnswers::add);

        // Step 2: Add highest-scoring answers with owner_reputation > 1000
        answers.stream()
                .filter(a -> a.getOwnerReputation() > 1000)
                .filter(a -> !selectedAnswers.contains(a))
                .sorted(Comparator.comparingInt(Answer::getScore).reversed())
                .limit(3 - selectedAnswers.size())
                .forEach(selectedAnswers::add);

        // Step 3: If we still need more, add remaining highest-scoring answers
        if (selectedAnswers.size() < 3) {
            answers.stream()
                    .filter(a -> !selectedAnswers.contains(a))
                    .sorted(Comparator.comparingInt(Answer::getScore).reversed())
                    .limit(3 - selectedAnswers.size())
                    .forEach(selectedAnswers::add);
        }

        question.setAnswers(selectedAnswers);
    }
}