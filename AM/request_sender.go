package main

// Import the required packages
import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image/color"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

var iterations int = 500
var actualIterations int = 1000

// Constants for API endpoints and file names
const (
	javaAPI               = "http://node0:8180/java"
	goAPI                 = "http://172.17.0.1:3234/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/GoLL"
	javaResponseTimesFile = "java_response_times.txt"
	goResponseTimesFile   = "go_response_times.txt"
	javaServerTimesFile   = "java_server_times.txt"
	goServerTimesFile     = "go_server_times.txt"
	goHeapFile            = "go_heap_memory.log"
	sshUser               = "am_CU"
	sshHost               = "node0"
	sshCommand            = "docker logs $(docker ps -q --filter ancestor=openwhisk/action-golang-v1.20:nightly)"
)

// Response structure for unmarshalling JSON data
type APIResponse struct {
	ExecutionTime int64 `json:"executionTime"`
	HeapAlloc     int64 `json:"heapAlloc"`
}

func main() {
	// Set a default value for arraysize
	defaultArraySize := 100
	arraysize := defaultArraySize

	// Check if a command line argument is provided
	if len(os.Args) > 1 {
		arraysizeStr := os.Args[1]
		if convertedSize, err := strconv.Atoi(arraysizeStr); err == nil {
			arraysize = convertedSize // Update only if conversion is successful
		} else {
			fmt.Printf("Invalid array size provided, using default value %d\n", defaultArraySize)
		}
	}
	fmt.Printf("\nArraysize: %d\n", arraysize)
	// ensure server is alive
	checkServerAlive(goAPI)
	// Warm up
	goResponseTimes, goServerTimes, heapSizes := sendRequests(goAPI, arraysize)
	iterations = actualIterations
	fmt.Printf("Warm up done, starting plotting run\n")
	// Actual measurements
	goResponseTimes, goServerTimes, heapSizes = sendRequests(goAPI, arraysize)
	_ = plotTimes(goResponseTimes, heapSizes, fmt.Sprintf("Server Times for Arraysize %d", arraysize))
	writeTimesToFile(goResponseTimesFile, goResponseTimes)
	writeTimesToFile(goServerTimesFile, goServerTimes)
	writeTimesToFile(goHeapFile, heapSizes)

	// Analyze OpenWhisk logs to count requests per container
	containerRequestCounts := getContainerRequestCounts()
	for container, count := range containerRequestCounts {
		fmt.Printf("Container %s served %d requests\n", container, count)
	}
}

func getContainerRequestCounts() map[string]int {
	sshCmd := exec.Command("ssh", fmt.Sprintf("%s@%s", sshUser, sshHost), sshCommand)
	var out bytes.Buffer
	sshCmd.Stdout = &out
	err := sshCmd.Run()
	if err != nil {
		log.Fatalf("Failed to execute SSH command: %v", err)
	}

	logData := out.String()
	containerCounts := make(map[string]int)

	// Parse logs and count requests per container
	lines := strings.Split(logData, "\n")
	for _, line := range lines {
		if strings.Contains(line, "activationId") {
			// Extract container ID or name
			containerID := extractContainerID(line)
			if containerID != "unknown" {
				containerCounts[containerID]++
			}
		}
	}
	return containerCounts
}

func extractContainerID(logLine string) string {
	// Example log line: "Container wsk0_5_guest_GoLL started"
	parts := strings.Split(logLine, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "wsk") {
			return part
		}
	}
	return "unknown"
}

func plotSLA(times []int64) error {
	// Calculate percentiles
	sortedTimes := make([]float64, len(times))
	for i, v := range times {
		sortedTimes[i] = float64(v)
	}

	// Sort times for accurate percentile calculation
	sort.Float64s(sortedTimes)

	// Create a new plot
	p := plot.New()
	p.Title.Text = "Server Response Time Percentiles"
	p.X.Label.Text = "Percentile"
	p.Y.Label.Text = "Time (microseconds)"

	// Define percentiles to plot
	percentiles := []float64{0.50, 0.90, 0.95, 0.99, 0.999, 0.9999, 0.99999}
	percentileValues := make(plotter.XYs, len(percentiles))

	// Calculate percentile values
	for i, percentile := range percentiles {
		value := stat.Quantile(percentile, stat.Empirical, sortedTimes, nil)
		percentileValues[i].X = percentile * 100 // Convert to percentage
		percentileValues[i].Y = value
	}

	// Add percentiles to plot as points
	scatter, err := plotter.NewScatter(percentileValues)
	if err != nil {
		return err
	}
	scatter.GlyphStyle.Radius = vg.Points(3) // Set point size
	p.Add(scatter)

	// Save the plot to a PNG file
	if err := p.Save(8*vg.Inch, 4*vg.Inch, "sla_plot.png"); err != nil {
		return err
	}
	return nil
}

func plotTimes(times []int64, heapsizes []int64, title string) error {
	p := plot.New()
	p.Title.Text = "Performance"
	p.X.Label.Text = "Request"
	p.Y.Label.Text = "Time (microseconds)"

	pts := make(plotter.XYs, len(times))
	for i := range times {
		pts[i].X = float64(i)
		pts[i].Y = float64(times[i])
	}

	line, err := plotter.NewLine(pts)
	if err != nil {
		return err
	}
	line.Color = color.RGBA{R: 0, G: 0, B: 0, A: 255} // Black for time
	p.Add(line)
	if err := p.Save(8*vg.Inch, 4*vg.Inch, "times_plot.png"); err != nil {
		return err
	}

	p = plot.New()
	p.Title.Text = "Memory Use"
	p.X.Label.Text = "Request"
	p.Y.Label.Text = "Heap Utilization"

	// Heap size series (optional: scale if necessary)
	ptsHeapSizes := make(plotter.XYs, len(heapsizes))
	for i, size := range heapsizes {
		ptsHeapSizes[i].X = float64(i)
		// Scale heap size for visualization if needed
		ptsHeapSizes[i].Y = float64(size) // Consider scaling
	}
	lineHeapSizes, err := plotter.NewLine(ptsHeapSizes)
	if err != nil {
		return err
	}
	line.Color = color.RGBA{R: 0, G: 0, B: 255, A: 255} // Blue for heap
	p.Add(lineHeapSizes)

	if err := p.Save(8*vg.Inch, 4*vg.Inch, "heap_plot.png"); err != nil {
		return err
	}

	return nil
}

func sendRequests(apiURL string, arraysize int) ([]int64, []int64, []int64) {
	var responseTimes []int64
	var serverTimes []int64
	var heapSizes []int64

	for i := 0; i < iterations; i++ {
		seed := rand.Intn(10000) // Example seed generation
		requestURL1 := fmt.Sprintf("%s?seed=%d", apiURL, seed)
		requestURL2 := fmt.Sprintf("%s&arraysize=%d", requestURL1, arraysize)
		requestURL := fmt.Sprintf("%s&requestnumber=%d", requestURL2, i)

		startTime := time.Now()
		resp, err := http.Get(requestURL)
		if err != nil {
			fmt.Println("Error sending request:", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Println("Non-OK HTTP status code:", resp.StatusCode)
		}

		// Read and unmarshal the response body
		responseBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			continue
		}

		var apiResp APIResponse
		if err := json.Unmarshal(responseBody, &apiResp); err != nil {
			fmt.Println("Error unmarshalling response:", err)
			fmt.Println("Response body:", string(responseBody))
			continue
		}

		endTime := time.Now()
		elapsed := endTime.Sub(startTime)

		responseTimes = append(responseTimes, elapsed.Microseconds())
		serverTimes = append(serverTimes, apiResp.ExecutionTime)
		// fmt.Println("Time:", apiResp.ExecutionTime)
		// Collect usedHeapSize along with other metrics
		// fmt.Println("UsedHeapSize:", apiResp.UsedHeapSize)
		heapSizes = append(heapSizes, apiResp.HeapAlloc)
	}

	return responseTimes, serverTimes, heapSizes
}

func checkServerAlive(apiURL string) {
	fmt.Println("Checking server for heartbeat.")
	for i := 0; i < iterations/10; i++ {
		seed := rand.Intn(10000) // Random seed generation
		arraysize := 10          // Do not pollute memory for aliveCheck
		requestURL := fmt.Sprintf("%s?seed=%d&arraysize=%d", apiURL, seed, arraysize)
		resp, err := http.Get(requestURL)
		if err != nil {
			fmt.Println("Error sending request:", err)
			time.Sleep(time.Second)
			continue
		}
		// Check if the HTTP status code is 200 (OK)
		if resp.StatusCode == http.StatusOK {
			fmt.Println("OK Response received from server.")
			// Read and unmarshal the response body
			responseBody, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Println("Response: ", string(responseBody))
			// Break out of the loop if a correct response is received
			break
		} else {
			resp.Body.Close()
		}
	}
}

func writeTimesToFile(filename string, times []int64) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	for index, time := range times {
		_, err := file.WriteString(strconv.Itoa(index) + ", " + strconv.FormatInt(time, 10) + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}
}

func latencyAnalysis(fileName string, arraySize int, responseTimes, serverTimes []int64) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Helper function to calculate percentiles
	percentile := func(times []int64, p float64) int64 {
		if len(times) == 0 {
			return 0
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		index := int(float64(len(times)-1) * p)
		return times[index]
	}

	// Calculate statistics
	responseP50 := percentile(responseTimes, 0.50)
	responseP99 := percentile(responseTimes, 0.99)
	responseP999 := percentile(responseTimes, 0.999)
	responseP9999 := percentile(responseTimes, 0.9999)

	serverP50 := percentile(serverTimes, 0.50)
	serverP99 := percentile(serverTimes, 0.99)
	serverP999 := percentile(serverTimes, 0.999)
	serverP9999 := percentile(serverTimes, 0.9999)

	// Writing headers
	headers := []string{"ArraySize", "ResponseP50", "ResponseP99", "ResponseP999", "ResponseP9999", "ServerP50", "ServerP99", "ServerP999", "ServerP9999"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("error writing headers to csv: %v", err)
	}

	// Write the data to CSV
	record := []string{
		strconv.Itoa(arraySize),
		strconv.FormatInt(responseP50, 10),
		strconv.FormatInt(responseP99, 10),
		strconv.FormatInt(responseP999, 10),
		strconv.FormatInt(responseP9999, 10),
		strconv.FormatInt(serverP50, 10),
		strconv.FormatInt(serverP99, 10),
		strconv.FormatInt(serverP999, 10),
		strconv.FormatInt(serverP9999, 10),
	}

	if err := writer.Write(record); err != nil {
		return fmt.Errorf("error writing record to csv: %v", err)
	}

	return nil
}
