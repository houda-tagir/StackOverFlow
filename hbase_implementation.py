#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HBase Schema Implementation for Stack Overflow Data
--------------------------------------------------

This script provides helper functions to:
1. Create the HBase tables with appropriate schema
2. Write data to the tables
3. Query the tables for common access patterns

The schema is designed to efficiently store Stack Overflow question, answer, 
and trend data as specified in the requirements.
"""

import json
import time
from starbase import Connection
import happybase

# Configuration
HBASE_HOST = 'localhost'
HBASE_PORT = 9090  # Default HBase Thrift server port

# Connection setup
def get_connection():
    """Create a connection to HBase"""
    return happybase.Connection(HBASE_HOST, HBASE_PORT)

# Table creation
def create_tables(connection):
    """Create the HBase tables with appropriate schema"""
    # Define table names
    tables = {
        'stackoverflow_qna': {
            'question': dict(),  # Question metadata
            'answers': dict(),   # All answers
            'top_answers': dict()  # Top 3 answers for quick retrieval
        },
        'stackoverflow_trends': {
            'trend': dict()  # Trend metrics
        },
        'stackoverflow_tag_index': {
            'question_ids': dict()  # Index for tag -> question mapping
        }
    }
    
    existing_tables = set(connection.tables())
    
    # Create tables if they don't exist
    for table_name, families in tables.items():
        if table_name.encode() not in existing_tables:
            print(f"Creating table: {table_name}")
            connection.create_table(table_name, families)
            print(f"Table {table_name} created successfully")
        else:
            print(f"Table {table_name} already exists")

# Insert data functions
def insert_question(connection, question_data):
    """
    Insert a question and its answers into the stackoverflow_qna table
    
    Args:
        connection: HBase connection
        question_data: Dictionary with question data following the schema
    """
    table = connection.table('stackoverflow_qna')
    
    # Extract the question ID
    question_id = str(question_data['question_id'])
    
    # Process question data
    question_batch = {
        b'question:title': question_data['title'].encode(),
        b'question:body': question_data['body'].encode(),
        b'question:creation_date': str(question_data['creation_date']).encode(),
        b'question:score': str(question_data['score']).encode(),
        b'question:owner_reputation': str(question_data['owner_reputation']).encode(),
        b'question:tags': json.dumps(question_data['tags']).encode()
    }
    
    # Process answers
    has_accepted = False
    if 'answers' in question_data and question_data['answers']:
        # Check if any answer is accepted
        for answer in question_data['answers']:
            if answer.get('is_accepted', False):
                has_accepted = True
                break
                
        # Add all answers
        for i, answer in enumerate(question_data['answers'], 1):
            question_batch[f'answers:answer{i}_id'.encode()] = str(answer['answer_id']).encode()
            question_batch[f'answers:answer{i}_body'.encode()] = answer['body'].encode()
            question_batch[f'answers:answer{i}_score'.encode()] = str(answer['score']).encode()
            question_batch[f'answers:answer{i}_is_accepted'.encode()] = str(answer.get('is_accepted', False)).encode()
            question_batch[f'answers:answer{i}_owner_reputation'.encode()] = str(answer.get('owner_reputation', 0)).encode()
    
    question_batch[b'question:has_accepted'] = str(has_accepted).encode()
    question_batch[b'question:is_unanswered'] = str(not question_data.get('answers', [])).encode()
    
    # Select top 3 answers according to requirements
    top_answers = select_top_answers(question_data.get('answers', []))
    
    # Add top answers
    for i, answer in enumerate(top_answers, 1):
        question_batch[f'top_answers:top{i}_id'.encode()] = str(answer['answer_id']).encode()
        question_batch[f'top_answers:top{i}_body'.encode()] = answer['body'].encode()
        question_batch[f'top_answers:top{i}_score'.encode()] = str(answer['score']).encode()
        question_batch[f'top_answers:top{i}_is_accepted'.encode()] = str(answer.get('is_accepted', False)).encode()
        question_batch[f'top_answers:top{i}_owner_reputation'.encode()] = str(answer.get('owner_reputation', 0)).encode()
    
    # Insert the batch
    table.put(question_id.encode(), question_batch)
    
    # Update tag index
    tag_table = connection.table('stackoverflow_tag_index')
    creation_timestamp = str(question_data['creation_date'])
    
    for tag in question_data['tags']:
        tag_table.put(
            tag.encode(),
            {f'question_ids:{creation_timestamp}'.encode(): question_id.encode()}
        )

def select_top_answers(answers):
    """
    Select the top 3 answers according to requirements:
    1. If there's an accepted answer, include it first
    2. Then highest-scoring answers with owner_reputation > 1000
    3. If still under 3, fill with next-highest-score answers regardless of reputation
    
    Args:
        answers: List of answer dictionaries
        
    Returns:
        List of up to 3 top answers
    """
    if not answers:
        return []
    
    # First, find accepted answer if any
    top_answers = []
    accepted = None
    
    for answer in answers:
        if answer.get('is_accepted', False):
            accepted = answer
            break
    
    if accepted:
        top_answers.append(accepted)
    
    # Then, add highest scoring answers with reputation > 1000, excluding the accepted one
    high_rep_answers = [a for a in answers if 
                       a.get('owner_reputation', 0) > 1000 and 
                       a != accepted]
    high_rep_answers.sort(key=lambda x: x['score'], reverse=True)
    
    # Add until we have 3 answers or run out of high-rep answers
    for answer in high_rep_answers:
        if len(top_answers) >= 3:
            break
        top_answers.append(answer)
    
    # If still under 3, add highest scoring regardless of reputation
    if len(top_answers) < 3:
        remaining = [a for a in answers if a not in top_answers]
        remaining.sort(key=lambda x: x['score'], reverse=True)
        
        for answer in remaining:
            if len(top_answers) >= 3:
                break
            top_answers.append(answer)
    
    return top_answers

def insert_trend(connection, trend_data, period_type):
    """
    Insert trend data into the stackoverflow_trends table
    
    Args:
        connection: HBase connection
        trend_data: Dictionary with trend metrics
        period_type: One of 'hourly', 'daily', 'monthly'
    """
    table = connection.table('stackoverflow_trends')
    
    # Extract the tag and timestamp
    tag = trend_data['tag']
    timestamp = trend_data.get('timestamp', int(time.time()))
    
    # Format the timestamp based on the period type
    if period_type == 'hourly':
        formatted_time = time.strftime('%Y%m%d%H', time.localtime(timestamp))
    elif period_type == 'daily':
        formatted_time = time.strftime('%Y%m%d', time.localtime(timestamp))
    elif period_type == 'monthly':
        formatted_time = time.strftime('%Y%m', time.localtime(timestamp))
    else:
        raise ValueError(f"Invalid period type: {period_type}")
    
    # Create row key
    row_key = f"{tag}#{period_type}#{formatted_time}"
    
    # Prepare batch data
    batch_data = {
        b'trend:total_questions': str(trend_data.get('total_questions', 0)).encode(),
        b'trend:unanswered_percent': str(trend_data.get('unanswered_percent', 0.0)).encode(),
        b'trend:accepted_percent': str(trend_data.get('accepted_percent', 0.0)).encode(),
        b'trend:avg_question_score': str(trend_data.get('avg_question_score', 0.0)).encode(),
        b'trend:avg_answer_score': str(trend_data.get('avg_answer_score', 0.0)).encode()
    }
    
    # Add raw count if available
    if 'count' in trend_data:
        batch_data[b'trend:raw_count'] = str(trend_data['count']).encode()
    
    # Insert the batch
    table.put(row_key.encode(), batch_data)

# Query functions
def get_question_by_id(connection, question_id):
    """
    Retrieve a question by its ID
    
    Args:
        connection: HBase connection
        question_id: ID of the question to retrieve
        
    Returns:
        Dictionary with the question data
    """
    table = connection.table('stackoverflow_qna')
    row = table.row(str(question_id).encode())
    
    if not row:
        return None
    
    # Process the row data
    question = {
        'question_id': int(question_id),
        'title': row.get(b'question:title', b'').decode(),
        'body': row.get(b'question:body', b'').decode(),
        'creation_date': int(row.get(b'question:creation_date', b'0').decode()),
        'score': int(row.get(b'question:score', b'0').decode()),
        'owner_reputation': int(row.get(b'question:owner_reputation', b'0').decode()),
        'tags': json.loads(row.get(b'question:tags', b'[]').decode()),
        'has_accepted': row.get(b'question:has_accepted', b'False').decode() == 'True',
        'is_unanswered': row.get(b'question:is_unanswered', b'True').decode() == 'True'
    }
    
    # Get top answers
    top_answers = []
    for i in range(1, 4):  # Up to 3 top answers
        top_id_key = f'top_answers:top{i}_id'.encode()
        if top_id_key in row:
            answer = {
                'answer_id': int(row.get(top_id_key).decode()),
                'body': row.get(f'top_answers:top{i}_body'.encode(), b'').decode(),
                'score': int(row.get(f'top_answers:top{i}_score'.encode(), b'0').decode()),
                'is_accepted': row.get(f'top_answers:top{i}_is_accepted'.encode(), b'False').decode() == 'True',
                'owner_reputation': int(row.get(f'top_answers:top{i}_owner_reputation'.encode(), b'0').decode())
            }
            top_answers.append(answer)
    
    question['top_answers'] = top_answers
    return question

def get_questions_by_tag(connection, tag, limit=10, start_time=None, end_time=None):
    """
    Retrieve questions by tag with optional time filters
    
    Args:
        connection: HBase connection
        tag: Tag to search for
        limit: Maximum number of questions to return
        start_time: Optional start timestamp
        end_time: Optional end timestamp
        
    Returns:
        List of question dictionaries
    """
    tag_table = connection.table('stackoverflow_tag_index')
    
    # Set up scan parameters
    if start_time and end_time:
        row_start = f'question_ids:{start_time}'.encode()
        row_stop = f'question_ids:{end_time}'.encode()
        scan_filter = f"ColumnRangeFilter('{row_start}', true, '{row_stop}', true)"
    else:
        scan_filter = None
    
    # Get question IDs for the tag
    question_ids = []
    for key, data in tag_table.scan(row_prefix=tag.encode(), filter=scan_filter, limit=limit):
        for col, value in data.items():
            question_ids.append(value.decode())
    
    # Retrieve questions
    questions = []
    qna_table = connection.table('stackoverflow_qna')
    for qid in question_ids:
        questions.append(get_question_by_id(connection, qid))
    
    return questions

def get_tag_trends(connection, tag, period_type, start_time=None, end_time=None):
    """
    Retrieve trend data for a tag over time
    
    Args:
        connection: HBase connection
        tag: Tag to get trends for
        period_type: One of 'hourly', 'daily', 'monthly'
        start_time: Optional start formatted time (in the same format as period_type)
        end_time: Optional end formatted time
        
    Returns:
        List of trend dictionaries
    """
    table = connection.table('stackoverflow_trends')
    
    # Set up row prefix
    row_prefix = f"{tag}#{period_type}#"
    
    # Set up row start and stop if time range is specified
    row_start = None
    row_stop = None
    
    if start_time:
        row_start = f"{row_prefix}{start_time}"
    else:
        row_start = row_prefix
    
    if end_time:
        row_stop = f"{row_prefix}{end_time}~"  # ~ is higher than any ASCII character
    
    # Scan the table
    trends = []
    for key, data in table.scan(row_start=row_start.encode(), row_stop=row_stop.encode() if row_stop else None):
        # Decode the row key
        row_key = key.decode()
        parts = row_key.split('#')
        tag = parts[0]
        period = parts[1]
        timestamp = parts[2]
        
        trend = {
            'tag': tag,
            'period': period,
            'timestamp': timestamp,
            'total_questions': int(data.get(b'trend:total_questions', b'0').decode()),
            'unanswered_percent': float(data.get(b'trend:unanswered_percent', b'0.0').decode()),
            'accepted_percent': float(data.get(b'trend:accepted_percent', b'0.0').decode()),
            'avg_question_score': float(data.get(b'trend:avg_question_score', b'0.0').decode()),
            'avg_answer_score': float(data.get(b'trend:avg_answer_score', b'0.0').decode())
        }
        
        if b'trend:raw_count' in data:
            trend['raw_count'] = int(data[b'trend:raw_count'].decode())
            
        trends.append(trend)
    
    return trends

# Example usage
if __name__ == "__main__":
    # Connect to HBase
    conn = get_connection()
    
    # Create tables
    create_tables(conn)
    
    # Example: Insert a question
    example_question = {
        "question_id": 12345,
        "title": "How to connect Spark to HBase?",
        "body": "<p>I'm trying to use Spark to read data from HBase but facing issues with the configuration.</p>",
        "creation_date": 1654012800,
        "score": 25,
        "tags": ["spark", "hbase", "java"],
        "owner_reputation": 3500,
        "answers": [
            {
                "answer_id": 98765,
                "body": "<p>You need to use the HBase connector for Spark...</p>",
                "score": 15,
                "is_accepted": True,
                "owner_reputation": 12500
            },
            {
                "answer_id": 98766,
                "body": "<p>Another approach is to use...</p>",
                "score": 8,
                "is_accepted": False,
                "owner_reputation": 7800
            }
        ]
    }
    
    # Insert the question
    insert_question(conn, example_question)
    
    # Example: Insert trend data
    example_trend = {
        "tag": "spark",
        "total_questions": 1250,
        "unanswered_percent": 22.4,
        "accepted_percent": 45.6,
        "avg_question_score": 3.7,
        "avg_answer_score": 4.2,
        "count": 1500,
        "timestamp": 1654012800  # June 1, 2022
    }
    
    # Insert the trend data
    insert_trend(conn, example_trend, 'monthly')
    
    # Query examples
    print("Retrieving question by ID:")
    q = get_question_by_id(conn, 12345)
    print(f"Title: {q['title']}")
    print(f"Top answers: {len(q['top_answers'])}")
    
    print("\nRetrieving questions by tag:")
    qs = get_questions_by_tag(conn, "spark", limit=2)
    for q in qs:
        print(f"Question: {q['title']}")
    
    print("\nRetrieving tag trends:")
    trends = get_tag_trends(conn, "spark", "monthly", "202206", "202207")
    for t in trends:
        print(f"Period: {t['timestamp']}, Total questions: {t['total_questions']}")