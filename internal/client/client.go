package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/buberlo/apple-pod-control/internal/model"
)

type Config struct {
	Server             string
	Token              string
	CAFile             string
	InsecureSkipVerify bool
	Timeout            time.Duration
}

type Client struct {
	baseURL *url.URL
	token   string
	http    *http.Client
}

type StatusError struct {
	Code    int
	Reason  string
	Message string
}

func (e *StatusError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("API request failed with status %d", e.Code)
}

func New(config Config) (*Client, error) {
	if config.Server == "" {
		config.Server = "http://127.0.0.1:8080"
	}
	baseURL, err := url.Parse(config.Server)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return nil, fmt.Errorf("invalid server URL %q", config.Server)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: config.InsecureSkipVerify} // #nosec G402 -- explicit CLI option
	if config.CAFile != "" {
		data, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &Client{baseURL: baseURL, token: config.Token, http: &http.Client{Transport: transport, Timeout: config.Timeout}}, nil
}

func (c *Client) Apply(ctx context.Context, namespace string, deployment model.Deployment) (model.Deployment, error) {
	var result model.Deployment
	path := fmt.Sprintf("/apis/apc.dev/v1alpha1/namespaces/%s/deployments/%s", url.PathEscape(namespace), url.PathEscape(deployment.Metadata.Name))
	err := c.do(ctx, http.MethodPut, path, deployment, &result)
	return result, err
}

func (c *Client) GetDeployment(ctx context.Context, namespace, name string) (model.Deployment, error) {
	var result model.Deployment
	path := fmt.Sprintf("/apis/apc.dev/v1alpha1/namespaces/%s/deployments/%s", url.PathEscape(namespace), url.PathEscape(name))
	err := c.do(ctx, http.MethodGet, path, nil, &result)
	return result, err
}

func (c *Client) ListDeployments(ctx context.Context, namespace string) ([]model.Deployment, error) {
	var result struct {
		Items []model.Deployment `json:"items"`
	}
	path := fmt.Sprintf("/apis/apc.dev/v1alpha1/namespaces/%s/deployments", url.PathEscape(namespace))
	err := c.do(ctx, http.MethodGet, path, nil, &result)
	return result.Items, err
}

func (c *Client) DeleteDeployment(ctx context.Context, namespace, name string) error {
	path := fmt.Sprintf("/apis/apc.dev/v1alpha1/namespaces/%s/deployments/%s", url.PathEscape(namespace), url.PathEscape(name))
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

func (c *Client) ListPods(ctx context.Context, namespace string) ([]model.Workload, error) {
	var result struct {
		Items []model.Workload `json:"items"`
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods", url.PathEscape(namespace))
	err := c.do(ctx, http.MethodGet, path, nil, &result)
	return result.Items, err
}

func (c *Client) ListNodes(ctx context.Context) ([]model.Node, error) {
	var result struct {
		Items []model.Node `json:"items"`
	}
	err := c.do(ctx, http.MethodGet, "/api/v1/nodes", nil, &result)
	return result.Items, err
}

func (c *Client) Version(ctx context.Context) (map[string]any, error) {
	result := map[string]any{}
	err := c.do(ctx, http.MethodGet, "/version", nil, &result)
	return result, err
}

func (c *Client) do(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("contact API server: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read API response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var status struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &status)
		if status.Message == "" {
			status.Message = strings.TrimSpace(string(data))
		}
		return &StatusError{Code: response.StatusCode, Reason: status.Reason, Message: status.Message}
	}
	if output == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("decode API response: %w", err)
	}
	return nil
}

func IsNotFound(err error) bool {
	var statusError *StatusError
	return errors.As(err, &statusError) && statusError.Code == http.StatusNotFound
}
