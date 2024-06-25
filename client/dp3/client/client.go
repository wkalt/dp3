package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	serverURL string
}

func New(serverURL string) *Client {
	return &Client{
		serverURL: serverURL,
	}
}

func (c *Client) StreamImport(
	ctx context.Context,
	database string,
	producer string,
	r io.Reader,
) error {
	url := fmt.Sprintf("%s/databases/%s/producers/%s/import", c.serverURL, database, producer)
	resp, err := http.Post(url, "application/octet-stream", r)
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
