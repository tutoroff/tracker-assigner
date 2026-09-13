package notifier

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestTelegramNotifier_NotifySuccess(t *testing.T) {
	var receivedPayload telegramPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "bot12345/sendMessage") {
			t.Errorf("unexpected URL path: %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&receivedPayload)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	notifier := &TelegramNotifier{
		botToken:   "12345",
		chatID:     "-100123456",
		httpClient: server.Client(),
		logger:     zap.NewNop(),
	}

	notifier.httpClient.Transport = &rewriteTransport{
		targetHost: server.URL,
		wrapped:    server.Client().Transport,
	}

	err := notifier.Notify(context.Background(), "DLQ Error", "Ticket MARKETING-1 failed after 5 retries")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedPayload.ChatID != "-100123456" {
		t.Errorf("expected chat_id -100123456, got %s", receivedPayload.ChatID)
	}
	if !strings.Contains(receivedPayload.Text, "DLQ Error") {
		t.Errorf("expected text to contain 'DLQ Error', got %s", receivedPayload.Text)
	}
	if receivedPayload.ParseMode != "HTML" {
		t.Errorf("expected ParseMode HTML, got %s", receivedPayload.ParseMode)
	}
}

func TestTelegramNotifier_TitleFormatting(t *testing.T) {
	tests := []struct {
		name         string
		title        string
		message      string
		expectedText string
	}{
		{
			name:         "empty title",
			title:        "",
			message:      "Simple message body",
			expectedText: "Simple message body",
		},
		{
			name:         "title with <b> html tag",
			title:        "<b>Bold Title</b>",
			message:      "Ticket assigned",
			expectedText: "<b>Bold Title</b>\n\nTicket assigned",
		},
		{
			name:         "title with target emoji prefix",
			title:        "🎯 New Assignment",
			message:      "Assigned to Alice",
			expectedText: "🎯 New Assignment\n\nAssigned to Alice",
		},
		{
			name:         "title with clipboard emoji prefix",
			title:        "📋 Queue Status",
			message:      "5 tickets pending",
			expectedText: "📋 Queue Status\n\n5 tickets pending",
		},
		{
			name:         "title with warning emoji prefix",
			title:        "⚠️ High Load",
			message:      "WIP limit reached",
			expectedText: "⚠️ High Load\n\nWIP limit reached",
		},
		{
			name:         "plain title formatted with warning badge and bold",
			title:        "System Alert",
			message:      "Server restarted",
			expectedText: "⚠️ <b>System Alert</b>\n\nServer restarted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var received telegramPayload
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&received)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer server.Close()

			n := NewTelegramNotifier("test-token", "-999", zap.NewNop())
			n.httpClient = server.Client()
			n.httpClient.Transport = &rewriteTransport{
				targetHost: server.URL,
				wrapped:    server.Client().Transport,
			}

			err := n.NotifyToChat(context.Background(), "-999", tc.title, tc.message)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if received.Text != tc.expectedText {
				t.Errorf("got %q, want %q", received.Text, tc.expectedText)
			}
		})
	}
}

func TestTelegramNotifier_NotifyToChat_SpecificChatID(t *testing.T) {
	var received telegramPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := NewTelegramNotifier("bot-token", "default-chat", zap.NewNop())
	n.httpClient = server.Client()
	n.httpClient.Transport = &rewriteTransport{
		targetHost: server.URL,
		wrapped:    server.Client().Transport,
	}

	targetChat := "-555444333"
	err := n.NotifyToChat(context.Background(), targetChat, "Hello", "World")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.ChatID != targetChat {
		t.Errorf("expected chatID %s, got %s", targetChat, received.ChatID)
	}
}

func TestTelegramNotifier_EmptyTokens_NoOp(t *testing.T) {
	// Empty botToken
	n1 := NewTelegramNotifier("", "12345", zap.NewNop())
	if err := n1.Notify(context.Background(), "Title", "Msg"); err != nil {
		t.Errorf("expected nil error for empty botToken, got %v", err)
	}
	if err := n1.NotifyToChat(context.Background(), "12345", "Title", "Msg"); err != nil {
		t.Errorf("expected nil error for empty botToken in NotifyToChat, got %v", err)
	}

	// Empty chatID
	n2 := NewTelegramNotifier("valid-token", "", zap.NewNop())
	if err := n2.Notify(context.Background(), "Title", "Msg"); err != nil {
		t.Errorf("expected nil error for empty chatID, got %v", err)
	}
	if err := n2.NotifyToChat(context.Background(), "", "Title", "Msg"); err != nil {
		t.Errorf("expected nil error for empty chatID in NotifyToChat, got %v", err)
	}
}

func TestTelegramNotifier_APIError_Non200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: chat not found"}`))
	}))
	defer server.Close()

	n := NewTelegramNotifier("bot-token", "bad-chat", zap.NewNop())
	n.httpClient = server.Client()
	n.httpClient.Transport = &rewriteTransport{
		targetHost: server.URL,
		wrapped:    server.Client().Transport,
	}

	err := n.Notify(context.Background(), "Alert", "Failed")
	if err == nil {
		t.Fatal("expected error on HTTP 400, got nil")
	}
	if !strings.Contains(err.Error(), "telegram API returned status 400") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestTelegramNotifier_TransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	serverURL := server.URL
	server.Close() // immediately close to cause network failure

	n := NewTelegramNotifier("bot-token", "chat", zap.NewNop())
	n.httpClient = &http.Client{
		Transport: &rewriteTransport{
			targetHost: serverURL,
		},
	}

	err := n.Notify(context.Background(), "Alert", "Network test")
	if err == nil {
		t.Fatal("expected error on unreachable server, got nil")
	}
	if !strings.Contains(err.Error(), "failed to send telegram notification") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNopNotifier_Full(t *testing.T) {
	nop := &NopNotifier{}
	if err := nop.Notify(context.Background(), "Test", "Msg"); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := nop.NotifyToChat(context.Background(), "chat-123", "Test", "Msg"); err != nil {
		t.Errorf("expected nil error from NotifyToChat, got %v", err)
	}
}

func TestTelegramNotifier_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := NewTelegramNotifier("bot-token", "chat-id", zap.NewNop())
	n.httpClient = server.Client()
	n.httpClient.Transport = &rewriteTransport{
		targetHost: server.URL,
		wrapped:    server.Client().Transport,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := n.Notify(ctx, "Title", "Message")
	if err == nil {
		t.Fatal("expected error on canceled context, got nil")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected 'context canceled' in error, got: %v", err)
	}
}

type rewriteTransport struct {
	targetHost string
	wrapped    http.RoundTripper
}

func (r *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(r.targetHost, "http://")
	if r.wrapped != nil {
		return r.wrapped.RoundTrip(req)
	}
	return http.DefaultTransport.RoundTrip(req)
}
