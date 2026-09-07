package debrid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func doForm(ctx context.Context, c *http.Client, method, endpoint, token string, v url.Values) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(v.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: debrid API transport failure", ErrTransient)
	}
	return decodeAPIResponse(resp, "alldebrid")
}

func decodeAPIResponse(resp *http.Response, provider string) (map[string]any, error) {
	defer resp.Body.Close()
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, NewRateLimitError(provider+" API", retryAfter(resp.Header.Get("Retry-After")), strings.TrimSpace(string(b)))
	}
	if resp.StatusCode >= 500 {
		return nil, NewProviderUnavailableError(provider+" API", retryAfter(resp.Header.Get("Retry-After")), strings.TrimSpace(string(b)))
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("debrid API %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if readErr != nil {
		return nil, fmt.Errorf("%w: debrid API response interrupted", ErrTransient)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%w: invalid debrid API response", ErrTransient)
	}
	if ok, exists := out["success"].(bool); exists && !ok {
		return nil, fmt.Errorf("debrid API: %v", out["detail"])
	}
	if s, _ := out["status"].(string); s == "error" {
		return nil, allDebridAPIError(object(out["error"]))
	}
	return out, nil
}

func allDebridAPIError(detail map[string]any) error {
	code := str(detail["code"])
	switch code {
	case "LINK_DOWN", "MAGNET_INVALID_ID", "MAGNET_LINKS_REMOVED":
		return fmt.Errorf("%w: alldebrid %s", ErrStaleItem, code)
	case "LINK_TEMPORARY_UNAVAILABLE", "LINK_HOST_UNAVAILABLE":
		return fmt.Errorf("%w: alldebrid %s", ErrTransient, code)
	default:
		return fmt.Errorf("alldebrid API: %s: %s", code, str(detail["message"]))
	}
}
func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func array(v any) []any           { a, _ := v.([]any); return a }
func str(v any) string            { s, _ := v.(string); return s }
func num(v any) int64             { n, _ := v.(float64); return int64(n) }
