package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	nodes  []string
	leader string
	quit   bool
}

func NewClient(node string) *Client {
	c := &Client{
		nodes:  []string{node},
		leader: node,
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

	resp, err := http.Post(c.leader+"/events", "application/json", reader)
	if err != nil {
		if len(c.nodes) == 1 && c.nodes[0] == c.leader {
			return err
		}

		c.leader = c.nodes[rand.Intn(len(c.nodes))]
		fmt.Println("error when connecting to leader", err, "switching to", c.leader)
		return c.Event(content, indexes)
	}

	defer resp.Body.Close()

	leader := resp.Header.Get("Cluster-Leader")

	if resp.StatusCode == 400 && leader != "" {
		c.leader = leader
		return c.Event(content, indexes)
	}

	return e
}

func (c *Client) Close() {
	c.quit = true
}

func (c *Client) refreshNodes() error {
	resp, err := http.Get(c.nodes[0] + "/cluster/status")
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	nodes := strings.Split(resp.Header.Get("Cluster-Nodes"), ",")

	if len(nodes) > 0 {
		c.nodes = nodes
	}

	return nil
}
