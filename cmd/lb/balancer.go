package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	serverHealthStatus = make(map[string]bool)
)

func proactiveServerCheck() {
	for {
		for i := range serversPool {
			server := serversPool[i]
			if health(server) {
				serverHealthStatus[server] = true
			} else {
				serverHealthStatus[server] = false
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func isServerHealthy(server string) bool {
	return serverHealthStatus[server]
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func hash(input string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(input))
	return h.Sum32()
}

func balance(urlPath string) string {
	var healthyServers []string
	for _, server := range serversPool {
		if isServerHealthy(server) {
			healthyServers = append(healthyServers, server)
		}
	}

	if len(healthyServers) == 0 {
		log.Println("No servers available")
		return ""
	}

	serverIndex := int(hash(urlPath) % uint32(len(healthyServers)))
	return healthyServers[serverIndex]
}

func main() {
	flag.Parse()

	// TODO: Використовуйте дані про стан сервреа, щоб підтримувати список тих серверів, яким можна відправляти ззапит.
	go proactiveServerCheck()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// TODO: Рееалізуйте свій алгоритм балансувальника.
		server := balance(r.URL.Path)
		forward(server, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
