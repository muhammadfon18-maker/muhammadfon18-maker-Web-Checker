package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const workerCount = 10
const retryDelay = 500 * time.Millisecond
var maxAttempts = 3


type Result struct {
	URL        string
	StatusCode int
	Duration   time.Duration
	Attempts   int
	Err        error
}

type Job struct {
	Index int
	URL   string
}

func validateURL(rawURL string) error {
	parsedURL, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme: %q", parsedURL.Scheme)
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("URL must contain a host")
	}

	return nil

}

func worker(
	id int,
	jobs <-chan Job,
	results []Result,
	wg *sync.WaitGroup,
	client *http.Client,
) {

	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d: checking %s\n", id, job.URL)
		results[job.Index] = checkWebsite(client, job.URL)
	}

}

func checkWebsite(client * http.Client, rawURL string)Result{
	start := time.Now()

	result := Result{
		URL: rawURL,
	}

	for attemp := 1; attemp <= maxAttempts; attemp++{
		result.Attempts = attemp

		resp, err := client.Get(rawURL)
		if err != nil{
			result.Err = err

			if attemp < maxAttempts{
				time.Sleep(retryDelay)
				continue
			}

			break
		}

		shouldRetry := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500

		if !shouldRetry || attemp == maxAttempts{
			break
		}

		time.Sleep(retryDelay)
	}

	result.Duration = time.Since(start)

	return result
}

func main() { 

	s := time.Now()

	urls := os.Args[1:]

	if len(urls) == 0 {
		fmt.Println("Usage: go run . https://google.com https://github.com")
		return
	}

	results := make([]Result, len(urls))
	jobs := make(chan Job)
	var wg sync.WaitGroup

	for i := range urls {
		results[i].URL = urls[i]
	}

	validJobs := make([]Job, 0, len(urls))

	for i, rawURL := range urls {
		if err := validateURL(rawURL); err != nil {
			results[i] = Result{
				URL: rawURL,
				Err: err,
			}
			continue
		}

		validJobs = append(validJobs, Job{
			Index: i,
			URL:   rawURL,
		})
	}

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	workers := workerCount


	if len(validJobs) < workers {
		workers = len(validJobs)
	}

	for i := 0; i <= workers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg, &client)
	}

	go func() {
		defer close(jobs)

		for _, job := range validJobs {
			jobs <- job
		}
	}()

	wg.Wait()

	fmt.Println()
	fmt.Println("========== WEBSITE CHECKER ==========")
	fmt.Printf("%-32s %-12s %s\n", "URL", "STATUS", "TIME")
	fmt.Println("-------------------------------------")

	online := 0
	offline := 0

	for _, result := range results {
		if result.Err != nil {
			fmt.Printf(
				"%-32s %-12s %v\n",
				result.URL,
				"ERROR",
				result.Err,
			)
			offline++
			continue
		}

		status := "OFFLINE"

		if result.StatusCode >= 200 && result.StatusCode < 400 {
			status = "ONLINE"
			online++
		} else {
			offline++
		}

	fmt.Printf(
	"%-35s HTTP %-3d %-8s %8s attempts=%d\n",
	result.URL,
	result.StatusCode,
	status,
	result.Duration.Round(time.Millisecond),
	result.Attempts,
)
	}

	fmt.Println("-----------------------------------------------")

	fmt.Printf(
		"Total: %d | Online: %d | Offline: %d\n",
		len(results),
		online,
		offline,
	)

	fmt.Println("TOTOAL TIME", time.Since(s))

}
