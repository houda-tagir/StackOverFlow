package com.sightstack.controller;

import com.sightstack.dto.TrendData;
import com.sightstack.service.QuestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/trends")
@RequiredArgsConstructor
@Slf4j
public class TrendController {

    private final QuestionService questionService;

    @GetMapping
    public ResponseEntity<TrendData> getTrend(
            @RequestParam("tag") String tag,
            @RequestParam(name = "period", defaultValue = "day") String period) {

        log.info("Trend request: tag={}, period={}", tag, period);

        // Validate period
        if (!isValidPeriod(period)) {
            return ResponseEntity.badRequest().build();
        }

        TrendData trendData = questionService.getTrendData(tag, period);
        return ResponseEntity.ok(trendData);
    }

    private boolean isValidPeriod(String period) {
        return period.equalsIgnoreCase("hour") ||
                period.equalsIgnoreCase("day") ||
                period.equalsIgnoreCase("month");
    }
}