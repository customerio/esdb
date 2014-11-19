package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Nodes  []string
	Leader string
	quit   bool
}

type LocalClient struct {
	Node string
}

func NewClient(node string) *Client {
	c := &Client{
		Nodes:  []string{node},
		Leader: node,
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

func NewLocalClient(node string) *LocalClient {
	return &LocalClient{Node: node}
}

func (c *LocalClient) StreamsMetadata() (*Metadata, error) {
	resp, err := http.Get(c.Node + "/events/meta")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var meta Metadata
	err = json.Unmarshal(body, &meta)
	return &meta, err
}

func (c *Client) Compress(start, stop uint64) error {
	reader := strings.NewReader("")
	resp, err := http.Post(c.Leader+"/events/compress/"+strconv.FormatUint(start, 10)+"/"+strconv.FormatUint(stop, 10), "application/json", reader)

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

	return nil
}

func (c *Client) Event(content []byte, indexes map[string]string) error {
	body, _ := json.Marshal(map[string]interface{}{
		"body":    string(content),
		"indexes": indexes,
	})

	reader := strings.NewReader(string(body))

	resp, err := http.Post(c.Leader+"/events", "application/json", reader)
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

	return nil
}

func (c *Client) Close() {
	c.quit = true
}

func (c *Client) refreshNodes() error {
	resp, err := http.Get(c.Nodes[0] + "/cluster/status")
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
