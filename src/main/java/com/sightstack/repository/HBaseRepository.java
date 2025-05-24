package com.sightstack.repository;

import com.sightstack.model.Answer;
import com.sightstack.model.Question;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
@Slf4j
public class HBaseRepository {

    private final Connection hbaseConnection;

    @Value("${hbase.table.name}")
    private String tableName;

    private static final byte[] QUESTION_CF = Bytes.toBytes("q");
    private static final byte[] ANSWER_CF = Bytes.toBytes("a");
    private static final byte[] TREND_CF = Bytes.toBytes("t");

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    public List<Question> searchQuestions(String query, List<String> tags, int limit) {
        List<Question> results = new ArrayList<>();

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            // Add title filter if query is provided
            if (query != null && !query.isEmpty()) {
                SingleColumnValueFilter titleFilter = new SingleColumnValueFilter(
                        QUESTION_CF,
                        Bytes.toBytes("title"),
                        CompareFilter.CompareOp.EQUAL,
                        new SubstringComparator(query)
                );
                titleFilter.setFilterIfMissing(true);
                filterList.addFilter(titleFilter);
            }

            // Add tag filters if tags are provided
            if (tags != null && !tags.isEmpty()) {
                for (String tag : tags) {
                    SingleColumnValueFilter tagFilter = new SingleColumnValueFilter(
                            QUESTION_CF,
                            Bytes.toBytes("tags"),
                            CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(tag)
                    );
                    tagFilter.setFilterIfMissing(true);
                    filterList.addFilter(tagFilter);
                }
            }

            Scan scan = new Scan();
            scan.setFilter(filterList);
            scan.setCaching(limit);

            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                if (count >= limit) break;

                Question question = mapResultToQuestion(result);
                if (question != null) {
                    results.add(question);
                    count++;
                }
            }
            scanner.close();

        } catch (IOException e) {
            log.error("Error searching questions in HBase", e);
        }

        return results;
    }

    public List<String> suggestQuestionTitles(String prefix, int maxSuggestions) {
        List<String> suggestions = new ArrayList<>();

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            SingleColumnValueFilter titleFilter = new SingleColumnValueFilter(
                    QUESTION_CF,
                    Bytes.toBytes("title"),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(prefix)
            );
            titleFilter.setFilterIfMissing(true);

            Scan scan = new Scan();
            scan.setFilter(titleFilter);
            scan.setCaching(maxSuggestions);

            ResultScanner scanner = table.getScanner(scan);
            int count = 0;
            for (Result result : scanner) {
                if (count >= maxSuggestions) break;

                byte[] titleBytes = result.getValue(QUESTION_CF, Bytes.toBytes("title"));
                if (titleBytes != null) {
                    String title = Bytes.toString(titleBytes);
                    suggestions.add(title);
                    count++;
                }
            }
            scanner.close();

        } catch (IOException e) {
            log.error("Error suggesting question titles in HBase", e);
        }

        return suggestions;
    }

    private Question mapResultToQuestion(Result result) {
        byte[] titleBytes = result.getValue(QUESTION_CF, Bytes.toBytes("title"));
        byte[] bodyBytes = result.getValue(QUESTION_CF, Bytes.toBytes("body"));
        byte[] creationDateBytes = result.getValue(QUESTION_CF, Bytes.toBytes("creation_date"));
        byte[] scoreBytes = result.getValue(QUESTION_CF, Bytes.toBytes("score"));
        byte[] ownerReputationBytes = result.getValue(QUESTION_CF, Bytes.toBytes("owner_reputation"));
        byte[] tagsBytes = result.getValue(QUESTION_CF, Bytes.toBytes("tags"));

        if (titleBytes == null) return null;

        String rowKey = Bytes.toString(result.getRow());
        String title = Bytes.toString(titleBytes);
        String body = bodyBytes != null ? Bytes.toString(bodyBytes) : "";
        LocalDateTime creationDate = creationDateBytes != null ?
                LocalDateTime.parse(Bytes.toString(creationDateBytes), DATE_FORMATTER) :
                LocalDateTime.now();
        int score = scoreBytes != null ? Bytes.toInt(scoreBytes) : 0;
        int ownerReputation = ownerReputationBytes != null ? Bytes.toInt(ownerReputationBytes) : 0;
        List<String> tags = tagsBytes != null ?
                Arrays.asList(Bytes.toString(tagsBytes).split(",")) :
                new ArrayList<>();

        // Get answers for this question
        List<Answer> answers = getAnswersForQuestion(result);

        return Question.builder()
                .id(rowKey)
                .title(title)
                .body(body)
                .creationDate(creationDate)
                .score(score)
                .ownerReputation(ownerReputation)
                .tags(tags)
                .answers(answers)
                .build();
    }

    private List<Answer> getAnswersForQuestion(Result result) {
        List<Answer> answers = new ArrayList<>();

        // Answers are stored in different columns in the answer column family
        // with column qualifiers like a1, a2, a3, etc.
        for (int i = 1; i <= 10; i++) { // Assuming max 10 answers per question
            byte[] answerIdBytes = result.getValue(ANSWER_CF, Bytes.toBytes("a" + i + "_id"));
            if (answerIdBytes == null) continue;

            String answerId = Bytes.toString(answerIdBytes);
            byte[] bodyBytes = result.getValue(ANSWER_CF, Bytes.toBytes("a" + i + "_body"));
            byte[] scoreBytes = result.getValue(ANSWER_CF, Bytes.toBytes("a" + i + "_score"));
            byte[] isAcceptedBytes = result.getValue(ANSWER_CF, Bytes.toBytes("a" + i + "_is_accepted"));
            byte[] ownerReputationBytes = result.getValue(ANSWER_CF, Bytes.toBytes("a" + i + "_owner_reputation"));

            String body = bodyBytes != null ? Bytes.toString(bodyBytes) : "";
            int score = scoreBytes != null ? Bytes.toInt(scoreBytes) : 0;
            boolean isAccepted = isAcceptedBytes != null && Bytes.toBoolean(isAcceptedBytes);
            int ownerReputation = ownerReputationBytes != null ? Bytes.toInt(ownerReputationBytes) : 0;

            answers.add(Answer.builder()
                    .id(answerId)
                    .body(body)
                    .score(score)
                    .isAccepted(isAccepted)
                    .ownerReputation(ownerReputation)
                    .build());
        }

        return answers;
    }

    public List<Integer> getTrendData(String tag, String period, int pointCount) {
        List<Integer> trendData = new ArrayList<>();

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName + "_trends"))) {
            Get get = new Get(Bytes.toBytes(tag + "_" + period));
            Result result = table.get(get);

            if (!result.isEmpty()) {
                for (int i = 1; i <= pointCount; i++) {
                    byte[] pointBytes = result.getValue(TREND_CF, Bytes.toBytes("p" + i));
                    if (pointBytes != null) {
                        int point = Bytes.toInt(pointBytes);
                        trendData.add(point);
                    } else {
                        trendData.add(0);
                    }
                }
            }

        } catch (IOException e) {
            log.error("Error fetching trend data from HBase", e);
        }

        return trendData;
    }
}