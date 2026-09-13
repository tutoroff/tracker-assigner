package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Notifier defines interface for sending system alerts.
type Notifier interface {
	Notify(ctx context.Context, title, message string) error
	NotifyToChat(ctx context.Context, chatID, title, message string) error
}

// NopNotifier does nothing (for disabled alerts or testing).
type NopNotifier struct{}

// Notify performs a no-op notification.
func (n *NopNotifier) Notify(ctx context.Context, title, message string) error {
	return nil
}

// NotifyToChat performs a no-op notification to a specific chat.
func (n *NopNotifier) NotifyToChat(ctx context.Context, chatID, title, message string) error {
	return nil
}

// TelegramNotifier sends notifications to a Telegram chat via Bot API.
type TelegramNotifier struct {
	botToken   string
	chatID     string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewTelegramNotifier creates a new TelegramNotifier.
func NewTelegramNotifier(botToken, chatID string, logger *zap.Logger) *TelegramNotifier {
	return &TelegramNotifier{
		botToken: botToken,
		chatID:   chatID,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

type telegramPayload struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

// Notify sends a formatted alert message to the configured Telegram chat.
func (t *TelegramNotifier) Notify(ctx context.Context, title, message string) error {
	if t.botToken == "" || t.chatID == "" {
		return nil
	}

	return t.NotifyToChat(ctx, t.chatID, title, message)
}

// NotifyToChat sends a formatted alert message to a specific Telegram chat.
func (t *TelegramNotifier) NotifyToChat(ctx context.Context, chatID, title, message string) error {
	if t.botToken == "" || chatID == "" {
		return nil
	}

	var text string
	if title == "" {
		text = message
	} else if strings.Contains(title, "<b>") || strings.HasPrefix(title, "🎯") || strings.HasPrefix(title, "📋") || strings.HasPrefix(title, "⚠️") {
		text = fmt.Sprintf("%s\n\n%s", title, message)
	} else {
		text = fmt.Sprintf("⚠️ <b>%s</b>\n\n%s", title, message)
	}

	payload := telegramPayload{
		ChatID:    chatID,
		Text:      text,
		ParseMode: "HTML",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal telegram payload: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send telegram notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram API returned status %d", resp.StatusCode)
	}

	return nil
}
