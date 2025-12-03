package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// fakeSQS implements sqsClient interface for testing
type fakeSQS struct {
	input  *sqs.SendMessageBatchInput
	output *sqs.SendMessageBatchOutput
	err    error
}

func (f *fakeSQS) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	f.input = input
	return f.output, f.err
}

// resetGlobals resets package-level globals between tests
func resetGlobals() {
	MessageCounter = 0
	SqsRecords = nil
	sqsOutLogLevel = 1 // default to info
}

// captureStdout captures stdout output during test execution
func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	_ = w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestCreateRecordString(t *testing.T) {
	tests := []struct {
		name      string
		timestamp time.Time
		tag       string
		record    map[interface{}]interface{}
		wantErr   bool
		validate  func(t *testing.T, result string)
	}{
		{
			name:      "basic record",
			timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			tag:       "test.tag",
			record: map[interface{}]interface{}{
				"message": "hello world",
				"level":   "info",
			},
			wantErr: false,
			validate: func(t *testing.T, result string) {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(result), &m); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				if m["@timestamp"] != "2024-01-15T10:30:00Z" {
					t.Errorf("unexpected timestamp: %v", m["@timestamp"])
				}
				if m["message"] != "hello world" {
					t.Errorf("unexpected message: %v", m["message"])
				}
				if m["level"] != "info" {
					t.Errorf("unexpected level: %v", m["level"])
				}
			},
		},
		{
			name:      "record with byte slice value",
			timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			tag:       "test.tag",
			record: map[interface{}]interface{}{
				"data": []byte("binary data"),
			},
			wantErr: false,
			validate: func(t *testing.T, result string) {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(result), &m); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				if m["data"] != "binary data" {
					t.Errorf("byte slice not converted to string: %v", m["data"])
				}
			},
		},
		{
			name:      "record with numeric values",
			timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			tag:       "test.tag",
			record: map[interface{}]interface{}{
				"count":   42,
				"ratio":   3.14,
				"enabled": true,
			},
			wantErr: false,
			validate: func(t *testing.T, result string) {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(result), &m); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				if m["count"] != float64(42) {
					t.Errorf("unexpected count: %v", m["count"])
				}
				if m["ratio"] != 3.14 {
					t.Errorf("unexpected ratio: %v", m["ratio"])
				}
				if m["enabled"] != true {
					t.Errorf("unexpected enabled: %v", m["enabled"])
				}
			},
		},
		{
			name:      "empty record",
			timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			tag:       "test.tag",
			record:    map[interface{}]interface{}{},
			wantErr:   false,
			validate: func(t *testing.T, result string) {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(result), &m); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				if len(m) != 1 {
					t.Errorf("expected only timestamp field, got %d fields", len(m))
				}
			},
		},
		{
			name:      "timestamp with nanoseconds",
			timestamp: time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC),
			tag:       "test.tag",
			record: map[interface{}]interface{}{
				"key": "value",
			},
			wantErr: false,
			validate: func(t *testing.T, result string) {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(result), &m); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				ts := m["@timestamp"].(string)
				if !strings.Contains(ts, "123456789") {
					t.Errorf("nanoseconds not preserved in timestamp: %v", ts)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()
			result, err := createRecordString(tt.timestamp, tt.tag, tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("createRecordString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestSetLogLevel(t *testing.T) {
	tests := []struct {
		name          string
		envValue      string
		expectedLevel int
	}{
		{"debug level", "debug", 0},
		{"DEBUG uppercase", "DEBUG", 0},
		{"Debug mixed case", "Debug", 0},
		{"info level", "info", 1},
		{"INFO uppercase", "INFO", 1},
		{"error level", "error", 2},
		{"ERROR uppercase", "ERROR", 2},
		{"empty defaults to info", "", 1},
		{"unknown defaults to info", "unknown", 1},
		{"warning defaults to info", "warning", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()
			_ = os.Setenv("SQS_OUT_LOG_LEVEL", tt.envValue)
			defer func() { _ = os.Unsetenv("SQS_OUT_LOG_LEVEL") }()

			setLogLevel()

			if sqsOutLogLevel != tt.expectedLevel {
				t.Errorf("setLogLevel() = %d, want %d", sqsOutLogLevel, tt.expectedLevel)
			}
		})
	}
}

func TestWriteDebugLog(t *testing.T) {
	tests := []struct {
		name        string
		logLevel    int
		message     string
		shouldPrint bool
	}{
		{"prints at debug level", 0, "test message", true},
		{"silent at info level", 1, "test message", false},
		{"silent at error level", 2, "test message", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()
			sqsOutLogLevel = tt.logLevel

			output := captureStdout(func() {
				writeDebugLog(tt.message)
			})

			hasOutput := len(output) > 0
			if hasOutput != tt.shouldPrint {
				t.Errorf("writeDebugLog() output = %v, shouldPrint = %v", hasOutput, tt.shouldPrint)
			}
			if tt.shouldPrint {
				if !strings.Contains(output, tt.message) {
					t.Errorf("output doesn't contain message: %s", output)
				}
				if !strings.Contains(output, "debug") {
					t.Errorf("output doesn't contain 'debug': %s", output)
				}
				if !strings.Contains(output, "sqs-out") {
					t.Errorf("output doesn't contain 'sqs-out': %s", output)
				}
			}
		})
	}
}

func TestWriteInfoLog(t *testing.T) {
	tests := []struct {
		name        string
		logLevel    int
		message     string
		shouldPrint bool
	}{
		{"prints at debug level", 0, "test message", true},
		{"prints at info level", 1, "test message", true},
		{"silent at error level", 2, "test message", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()
			sqsOutLogLevel = tt.logLevel

			output := captureStdout(func() {
				writeInfoLog(tt.message)
			})

			hasOutput := len(output) > 0
			if hasOutput != tt.shouldPrint {
				t.Errorf("writeInfoLog() output = %v, shouldPrint = %v", hasOutput, tt.shouldPrint)
			}
			if tt.shouldPrint {
				if !strings.Contains(output, tt.message) {
					t.Errorf("output doesn't contain message: %s", output)
				}
				if !strings.Contains(output, "info") {
					t.Errorf("output doesn't contain 'info': %s", output)
				}
			}
		})
	}
}

func TestWriteErrorLog(t *testing.T) {
	tests := []struct {
		name        string
		logLevel    int
		err         error
		shouldPrint bool
	}{
		{"prints at debug level", 0, errors.New("test error"), true},
		{"prints at info level", 1, errors.New("test error"), true},
		{"prints at error level", 2, errors.New("test error"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()
			sqsOutLogLevel = tt.logLevel

			output := captureStdout(func() {
				writeErrorLog(tt.err)
			})

			hasOutput := len(output) > 0
			if hasOutput != tt.shouldPrint {
				t.Errorf("writeErrorLog() output = %v, shouldPrint = %v", hasOutput, tt.shouldPrint)
			}
			if tt.shouldPrint {
				if !strings.Contains(output, tt.err.Error()) {
					t.Errorf("output doesn't contain error message: %s", output)
				}
				if !strings.Contains(output, "error") {
					t.Errorf("output doesn't contain 'error': %s", output)
				}
			}
		})
	}
}

func TestSendBatchToSqs(t *testing.T) {
	tests := []struct {
		name          string
		config        *sqsConfig
		records       []*sqs.SendMessageBatchRequestEntry
		mockOutput    *sqs.SendMessageBatchOutput
		mockErr       error
		wantErr       bool
		validateInput func(t *testing.T, input *sqs.SendMessageBatchInput)
	}{
		{
			name: "successful send",
			config: &sqsConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			},
			records: []*sqs.SendMessageBatchRequestEntry{
				{
					Id:          aws.String("msg-1"),
					MessageBody: aws.String(`{"message":"test"}`),
				},
			},
			mockOutput: &sqs.SendMessageBatchOutput{
				Successful: []*sqs.SendMessageBatchResultEntry{
					{Id: aws.String("msg-1")},
				},
			},
			mockErr: nil,
			wantErr: false,
			validateInput: func(t *testing.T, input *sqs.SendMessageBatchInput) {
				if *input.QueueUrl != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue" {
					t.Errorf("unexpected queue URL: %s", *input.QueueUrl)
				}
				if len(input.Entries) != 1 {
					t.Errorf("expected 1 entry, got %d", len(input.Entries))
				}
			},
		},
		{
			name: "send with multiple messages",
			config: &sqsConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			},
			records: []*sqs.SendMessageBatchRequestEntry{
				{Id: aws.String("msg-1"), MessageBody: aws.String(`{"id":1}`)},
				{Id: aws.String("msg-2"), MessageBody: aws.String(`{"id":2}`)},
				{Id: aws.String("msg-3"), MessageBody: aws.String(`{"id":3}`)},
			},
			mockOutput: &sqs.SendMessageBatchOutput{
				Successful: []*sqs.SendMessageBatchResultEntry{
					{Id: aws.String("msg-1")},
					{Id: aws.String("msg-2")},
					{Id: aws.String("msg-3")},
				},
			},
			mockErr: nil,
			wantErr: false,
			validateInput: func(t *testing.T, input *sqs.SendMessageBatchInput) {
				if len(input.Entries) != 3 {
					t.Errorf("expected 3 entries, got %d", len(input.Entries))
				}
			},
		},
		{
			name: "SQS returns error",
			config: &sqsConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			},
			records: []*sqs.SendMessageBatchRequestEntry{
				{Id: aws.String("msg-1"), MessageBody: aws.String(`{"message":"test"}`)},
			},
			mockOutput: nil,
			mockErr:    errors.New("SQS service error"),
			wantErr:    true,
		},
		{
			name: "partial failure - some messages failed",
			config: &sqsConfig{
				queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			},
			records: []*sqs.SendMessageBatchRequestEntry{
				{Id: aws.String("msg-1"), MessageBody: aws.String(`{"id":1}`)},
				{Id: aws.String("msg-2"), MessageBody: aws.String(`{"id":2}`)},
			},
			mockOutput: &sqs.SendMessageBatchOutput{
				Successful: []*sqs.SendMessageBatchResultEntry{
					{Id: aws.String("msg-1")},
				},
				Failed: []*sqs.BatchResultErrorEntry{
					{Id: aws.String("msg-2"), Code: aws.String("InternalError")},
				},
			},
			mockErr: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()

			fake := &fakeSQS{
				output: tt.mockOutput,
				err:    tt.mockErr,
			}
			tt.config.mySQS = fake

			err := sendBatchToSqs(tt.config, tt.records)

			if (err != nil) != tt.wantErr {
				t.Errorf("sendBatchToSqs() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.validateInput != nil && fake.input != nil {
				tt.validateInput(t, fake.input)
			}
		})
	}
}

func TestValidateBatchSize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		isValid bool
	}{
		{"valid: 1", "1", true},
		{"valid: 5", "5", true},
		{"valid: 10", "10", true},
		{"invalid: 0", "0", false},
		{"invalid: 11", "11", false},
		{"invalid: -1", "-1", false},
		{"invalid: 100", "100", false},
		{"invalid: not a number", "abc", false},
		{"invalid: empty", "", false},
		{"invalid: float", "5.5", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()

			isValid := validateBatchSize(tt.input)
			if isValid != tt.isValid {
				t.Errorf("validateBatchSize(%q) = %v, want %v", tt.input, isValid, tt.isValid)
			}
		})
	}
}

func TestValidateQueueConfig(t *testing.T) {
	tests := []struct {
		name                string
		queueURL            string
		queueRegion         string
		queueMessageGroupID string
		wantErr             bool
		errContains         string
	}{
		{
			name:        "valid standard queue",
			queueURL:    "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			queueRegion: "us-east-1",
			wantErr:     false,
		},
		{
			name:                "valid FIFO queue with group ID",
			queueURL:            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo",
			queueRegion:         "us-east-1",
			queueMessageGroupID: "group-1",
			wantErr:             false,
		},
		{
			name:        "missing queue URL",
			queueURL:    "",
			queueRegion: "us-east-1",
			wantErr:     true,
			errContains: "QueueUrl",
		},
		{
			name:        "missing queue region",
			queueURL:    "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
			queueRegion: "",
			wantErr:     true,
			errContains: "QueueRegion",
		},
		{
			name:        "FIFO queue without group ID",
			queueURL:    "https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo",
			queueRegion: "us-east-1",
			wantErr:     true,
			errContains: "QueueMessageGroupId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals()

			err := validateQueueConfig(tt.queueURL, tt.queueRegion, tt.queueMessageGroupID)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateQueueConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error should contain %q, got %q", tt.errContains, err.Error())
				}
			}
		})
	}
}

func TestSqsConfigFields(t *testing.T) {
	config := &sqsConfig{
		queueURL:            "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		queueMessageGroupID: "group-1",
		pluginTagAttribute:  "tag",
		proxyURL:            "http://proxy:8080",
		batchSize:           5,
	}

	if config.queueURL != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue" {
		t.Errorf("unexpected queueURL: %s", config.queueURL)
	}
	if config.queueMessageGroupID != "group-1" {
		t.Errorf("unexpected queueMessageGroupID: %s", config.queueMessageGroupID)
	}
	if config.pluginTagAttribute != "tag" {
		t.Errorf("unexpected pluginTagAttribute: %s", config.pluginTagAttribute)
	}
	if config.proxyURL != "http://proxy:8080" {
		t.Errorf("unexpected proxyURL: %s", config.proxyURL)
	}
	if config.batchSize != 5 {
		t.Errorf("unexpected batchSize: %d", config.batchSize)
	}
}

func TestMessageCounterAndSqsRecords(t *testing.T) {
	resetGlobals()

	if MessageCounter != 0 {
		t.Errorf("MessageCounter should be 0 after reset, got %d", MessageCounter)
	}
	if SqsRecords != nil {
		t.Errorf("SqsRecords should be nil after reset")
	}

	MessageCounter = 5
	SqsRecords = []*sqs.SendMessageBatchRequestEntry{
		{Id: aws.String("test")},
	}

	if MessageCounter != 5 {
		t.Errorf("MessageCounter should be 5, got %d", MessageCounter)
	}
	if len(SqsRecords) != 1 {
		t.Errorf("SqsRecords should have 1 entry, got %d", len(SqsRecords))
	}

	resetGlobals()

	if MessageCounter != 0 {
		t.Errorf("MessageCounter should be 0 after second reset, got %d", MessageCounter)
	}
	if SqsRecords != nil {
		t.Errorf("SqsRecords should be nil after second reset")
	}
}
