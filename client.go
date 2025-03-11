package raccoon_kv_client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	Url string
}

var RequestFailedErr = errors.New("")

func (c *Client) Get(ctx context.Context, key string) (data []byte, version string, err error) {
	return doRequest(ctx, fmt.Sprintf("%s/kv/%s", c.Url, key), "")
}

func (c *Client) Watch(ctx context.Context, key string, cb func([]byte)) {
	var lastVersion string

	requestUrl := fmt.Sprintf("%s/kv/%s?watch=%d", c.Url, key, 60)

	backoffSeconds := 1

	for {
		data, version, err := doRequest(ctx, requestUrl, lastVersion)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				slog.Info("context deadline exceeded, stopping watch")
				return
			}

			slog.Error("failed to query kv store, backing off", slog.String("err", err.Error()), slog.Int("backoff_seconds", backoffSeconds))

			select {
			case <-ctx.Done():
				slog.Info("context deadline exceeded, stopping watch")
				return
			case <-time.NewTimer(time.Second * time.Duration(backoffSeconds)).C:
			}

			if backoffSeconds < 60 {
				backoffSeconds = backoffSeconds * 2
			}
		} else if lastVersion != version {
			lastVersion = version
			cb(data)
		}
	}
}

func (c *Client) Put(ctx context.Context, key string, data []byte) error {
	request, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%s/kv/%s", c.Url, key), bytes.NewReader(data))
	if err != nil {
		return err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code %d%w", response.StatusCode, RequestFailedErr)
	}

	return nil
}

func doRequest(ctx context.Context, url string, lastKnownVersion string) (data []byte, version string, err error) {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, "", err
	}

	if lastKnownVersion != "" {
		request.Header.Set("if-none-match", lastKnownVersion)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, "", err
	}

	version = response.Header.Get("etag")
	if version == "" {
		return nil, "", errors.New("missing etag")
	}

	if response.StatusCode == http.StatusNotFound {
		return nil, version, nil
	}

	if response.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status code %d%w", response.StatusCode, RequestFailedErr)
	}

	data, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
	}

	return data, lastKnownVersion, nil
}
