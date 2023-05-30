package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type IntegrationSuite struct{}

var _ = Suite(&IntegrationSuite{})

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func (s *IntegrationSuite) TestBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	resp1, err := client.Get(fmt.Sprintf("%s/api/v1/some-data2", baseAddress))
	if err != nil {
		c.Error(err)
	}
	c.Check(resp1.Header.Get("lb-from"), Equals, "server1:8080")

	resp2, err := client.Get(fmt.Sprintf("%s/api/v1/some-data5", baseAddress))
	if err != nil {
		c.Error(err)
	}
	c.Check(resp2.Header.Get("lb-from"), Equals, "server2:8080")

	resp3, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	if err != nil {
		c.Error(err)
	}
	c.Check(resp3.Header.Get("lb-from"), Equals, "server3:8080")

	respr, err := client.Get(fmt.Sprintf("%s/api/v1/some-data2", baseAddress))
	if err != nil {
		c.Error(err)
	}
	c.Check(respr.Header.Get("lb-from"), Equals, "server1:8080")
}

func (s *IntegrationSuite) BenchmarkBalancer(c *C) {
  if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
    c.Skip("Integration test is not enabled")
  }

  for i := 0; i < c.N; i++ {
    _, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
    if err != nil {
      c.Error(err)
    }
  }
}
