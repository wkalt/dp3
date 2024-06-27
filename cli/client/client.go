package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	serverURL string
	httpc     *http.Client
}

func New(serverURL, sharedKey string) *Client {
	return &Client{
		serverURL: serverURL,
		httpc:     NewHTTPClient(sharedKey),
	}
}

func (c *Client) StreamImport(
	ctx context.Context,
	database string,
	producer string,
	r io.Reader,
) error {
	url := fmt.Sprintf("%s/databases/%s/producers/%s/import", c.serverURL, database, producer)
	resp, err := c.httpc.Post(url, "application/octet-stream", r)
	if err != nil {
		return fmt.Errorf("error calling import: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unexpected status code: %s", resp.Status)
		}
		return fmt.Errorf("failed to import: %s", body)
	}
	return nil
}

type transport struct {
	key string
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.key))
	return http.DefaultTransport.RoundTrip(req)
}

func newTransport(key string) *transport {
	return &transport{key: key}
}

func NewHTTPClient(sharedKey string) *http.Client {
	return &http.Client{
		Transport: newTransport(sharedKey),
	}
}
