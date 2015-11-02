package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Nodes  []string
	Leader string
	conns  pool
	quit   bool
	client *http.Client
}

type LocalClient struct {
	Node   string
	conns  pool
	client *http.Client
}

type pool struct {
	conns chan bool
}

func newPool(concurrency int) pool {
	conns := make(chan bool, concurrency)

	for i := 0; i < concurrency; i++ {
		conns <- true
	}

	return pool{conns}
}

func (p *pool) get() {
	<-p.conns
}

func (p *pool) release() {
	p.conns <- true
}

type OffsetResponse struct {
	Continuation string   `json:"continuation"`
	Metadata     Metadata `json:"meta"`
}

func NewClient(node string, concurrency int) *Client {
	c := &Client{
		Nodes:  []string{node},
		Leader: node,
		conns:  newPool(concurrency),
		client: &http.Client{Timeout: 30 * time.Second},
	}

	go (func() {
		for {
			if c.quit {
				return
			}

			c.refreshNodes()

			time.Sleep(10 * time.Second)
		}
	})()

	return c
}

func NewLocalClient(node string, concurrency int) *LocalClient {
	return &LocalClient{Node: node, conns: newPool(concurrency), client: &http.Client{Timeout: 30 * time.Second}}
}

func (c *LocalClient) StreamsMetadata() (*Metadata, error) {
	c.conns.get()
	defer c.conns.release()

	resp, err := c.client.Get(c.Node + "/events/meta")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, parseError(resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var meta Metadata
	err = json.Unmarshal(body, &meta)
	return &meta, err
}

func (c *LocalClient) Offset(index, value string) (*Metadata, string, error) {
	c.conns.get()
	defer c.conns.release()

	dest, err := url.Parse(c.Node)
	if err != nil {
		return nil, "", err
	}

	dest.Path += "/events/offset"
	parameters := url.Values{}
	parameters.Add("index", index)
	parameters.Add("value", value)
	dest.RawQuery = parameters.Encode()

	resp, err := c.client.Get(dest.String())
	if err != nil {
		return nil, "", err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, "", parseError(resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	var or OffsetResponse
	err = json.Unmarshal(body, &or)
	return &or.Metadata, or.Continuation, err
}

func (c *Client) Compress(start, stop uint64) error {
	c.conns.get()
	defer c.conns.release()

	reader := strings.NewReader("")
	resp, err := c.client.Post(c.Leader+"/events/compress/"+strconv.FormatUint(start, 10)+"/"+strconv.FormatUint(stop, 10), "application/json", reader)

	if err != nil {
		if len(c.Nodes) == 1 && c.Nodes[0] == c.Leader {
			return err
		}

		c.Leader = c.Nodes[rand.Intn(len(c.Nodes))]
		fmt.Println("error when connecting to leader", err, "switching to", c.Leader)
		return c.Compress(start, stop)
	}

	defer resp.Body.Close()

	leader := resp.Header.Get("Cluster-Leader")

	if resp.StatusCode == 400 && leader != "" {
		c.Leader = leader
		return c.Compress(start, stop)
	}

	if resp.StatusCode != 200 {
		return parseError(resp.Body)
	}

	return nil
}

func (c *Client) Event(content []byte, indexes map[string]string) error {
	c.conns.get()
	defer c.conns.release()

	body, _ := json.Marshal(map[string]interface{}{
		"body":    string(content),
		"indexes": indexes,
	})

	reader := strings.NewReader(string(body))

	resp, err := c.client.Post(c.Leader+"/events", "application/json", reader)
	if err != nil {
		if len(c.Nodes) == 1 && c.Nodes[0] == c.Leader {
			return err
		}

		c.Leader = c.Nodes[rand.Intn(len(c.Nodes))]
		fmt.Println("error when connecting to leader", err, "switching to", c.Leader)
		return c.Event(content, indexes)
	}

	defer resp.Body.Close()

	leader := resp.Header.Get("Cluster-Leader")

	if resp.StatusCode == 400 && leader != "" {
		c.Leader = leader
		return c.Event(content, indexes)
	}

	if resp.StatusCode != 200 {
		return parseError(resp.Body)
	}

	return nil
}

func (c *Client) Events(contents [][]byte, indexes []map[string]string) error {
	c.conns.get()
	defer c.conns.release()

	m := make([]map[string]interface{}, len(contents))

	for i, content := range contents {
		m[i] = map[string]interface{}{
			"body":    string(content),
			"indexes": indexes[i],
		}
	}

	body, _ := json.Marshal(m)

	reader := strings.NewReader(string(body))

	resp, err := c.client.Post(c.Leader+"/events", "application/json", reader)
	if err != nil {
		if len(c.Nodes) == 1 && c.Nodes[0] == c.Leader {
			return err
		}

		c.Leader = c.Nodes[rand.Intn(len(c.Nodes))]
		fmt.Println("error when connecting to leader", err, "switching to", c.Leader)
		return c.Events(contents, indexes)
	}

	defer resp.Body.Close()

	leader := resp.Header.Get("Cluster-Leader")

	if resp.StatusCode == 400 && leader != "" {
		c.Leader = leader
		return c.Events(contents, indexes)
	}

	if resp.StatusCode != 200 {
		return parseError(resp.Body)
	}

	return nil
}

func (c *Client) Close() {
	c.quit = true
}

func parseError(body io.Reader) error {
	b, _ := ioutil.ReadAll(body)
	return errors.New("Bad response from log: " + string(b))
}

func (c *Client) refreshNodes() error {
	resp, err := c.client.Get(c.Nodes[0] + "/cluster/status")
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	nodes := strings.Split(resp.Header.Get("Cluster-Nodes"), ",")

	if len(nodes) > 0 {
		c.Nodes = nodes
	}

	return nil
}
