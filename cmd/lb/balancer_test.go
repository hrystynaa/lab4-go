package main

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BalancerSuite struct{}

var _ = Suite(&BalancerSuite{})

func (s *BalancerSuite) TestBalancer(c *C) {

	healthchecker := &HealthChecker{}
	healthchecker.serverHealthStatus = map[string]bool{
		"server1:8080": true,
		"server2:8080": true,
		"server3:8080": true,
	}

	balancer := &LoadBalancer{}
	balancer.healthChecker = healthchecker

	server1 := balancer.balance("/check")
	server1secondTime := balancer.balance("/check")
	server2 := balancer.balance("/check2")
	server3 := balancer.balance("/check5")

	c.Assert(server1, Equals, "server1:8080")
	c.Assert(server1, Equals, server1secondTime)
	c.Assert(server2, Equals, "server2:8080")
	c.Assert(server3, Equals, "server3:8080")
}

func mockHealth(dst string) bool {
	if dst == "server1:8080" {
		return true
	} else if dst == "server2:8080" {
		return false
	}
	return false
}

func mockHealthAllTrue(dst string) bool {
	return true
}

func (s *BalancerSuite) TestHealthChecker(c *C) {
	healthChecker := &HealthChecker{}
	healthChecker.serverHealthStatus = map[string]bool{}
	healthChecker.health = mockHealth

	healthChecker.CheckAllServers()
	c.Assert(healthChecker.serverHealthStatus, DeepEquals,
		map[string]bool{"server1:8080": true, "server2:8080": false, "server3:8080": false})

	healthyServers := healthChecker.GetHealthyServers()
	c.Assert(healthyServers, DeepEquals, []string{"server1:8080"})

	healthChecker.health = mockHealthAllTrue
	healthChecker.CheckAllServers()
	healthyServers = healthChecker.GetHealthyServers()
	c.Assert(healthyServers, DeepEquals, []string{"server1:8080", "server2:8080", "server3:8080"})
}

func (s *BalancerSuite) TestNoAvailableServers(c *C) {
	healthchecker := &HealthChecker{}
	healthchecker.serverHealthStatus = map[string]bool{
		"server1:8080": false,
		"server2:8080": false,
		"server3:8080": false,
	}

	balancer := &LoadBalancer{}
	balancer.healthChecker = healthchecker

	server := balancer.balance("/check")

	c.Assert(server, Equals, "")
}
