package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	Nodes  []string
	Leader string
	quit   bool
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
