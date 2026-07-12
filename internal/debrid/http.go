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
	req, _ := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("debrid API %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out map[string]any
	if err = json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if ok, exists := out["success"].(bool); exists && !ok {
		return nil, fmt.Errorf("debrid API: %v", out["detail"])
	}
	if s, _ := out["status"].(string); s == "error" {
		return nil, fmt.Errorf("debrid API: %v", out["error"])
	}
	return out, nil
}
func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func array(v any) []any           { a, _ := v.([]any); return a }
func str(v any) string            { s, _ := v.(string); return s }
func num(v any) int64             { n, _ := v.(float64); return int64(n) }
