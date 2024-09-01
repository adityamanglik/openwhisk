package main

import (
	"container/list"
	"encoding/json"
	"math"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"time"
)

// Response defines the structure of the response returned by the action
type Response struct {
	Sum             int64  `json:"sum"`
	ExecutionTime   int64  `json:"executionTime"`
	RequestNumber   int    `json:"requestNumber"`
	ArraySize       int    `json:"arraysize"`
	HeapAllocMemory uint64 `json:"heapAllocMemory"`
	GOGC            string `json:"GOGC"`
	GOMEMLIMIT      string `json:"GOMEMLIMIT"`
	NextGC          uint64 `json:"NextGC"`
	NumGC           uint32 `json:"NumGC"`
	ParsedSeed      string `json:"parsedSeed,omitempty"`
	ParsedArraySize string `json:"parsedArraySize,omitempty"`
	ParsedReqNum    string `json:"parsedReqNum,omitempty"`
}

// init function to set up environment variables
func init() {
	// Set GOGC, controls the garbage collector target percentage.
	if err := os.Setenv("GOGC", "1000"); err != nil {
		panic(err)
	}

	// Set GOMEMLIMIT
	if err := os.Setenv("GOMEMLIMIT", "128M"); err != nil {
		panic(err)
	}

	// Set GOMAXPROCS for serial GC
	if err := os.Setenv("GOMAXPROCS", "1"); err != nil {
		panic(err)
	}
}

// Main is the function implementing the OpenWhisk action
func Main(obj map[string]interface{}) map[string]interface{} {
	seed := 42               // default seed value
	ARRAY_SIZE := 10000      // default array size value
	REQ_NUM := math.MaxInt32 // default request number
	response := Response{}

	// Parse query parameters if provided
	if query, ok := obj["__ow_query"].(string); ok && query != "" {
		response.ParsedSeed = "Found query, seed not parsed"
		response.ParsedArraySize = "Found query, arraysize not parsed"
		response.ParsedReqNum = "Found query, requestnumber not parsed"
		values, err := url.ParseQuery(query)
		if err == nil {
			if val, ok := values["seed"]; ok {
				seed, _ = strconv.Atoi(val[0])
			}

			if val, ok := values["arraysize"]; ok {
				ARRAY_SIZE, _ = strconv.Atoi(val[0])
			}

			if val, ok := values["requestnumber"]; ok {
				REQ_NUM, _ = strconv.Atoi(val[0])
			}
		}
	}

	start := time.Now()

	rand.Seed(int64(seed))

	// Create a new list and perform operations similar to the original server
	lst := list.New()

	for i := 0; i < ARRAY_SIZE; i++ {
		lst.PushFront(rand.Intn(seed)) // Insert integers directly

		// Stress GC with nested list
		if i%5 == 0 {
			nestedList := list.New()
			for j := 0; j < rand.Intn(5); j++ {
				nestedList.PushBack(rand.Intn(seed))
			}
			lst.PushBack(nestedList)
		}

		// Immediate removal after insertion to stress GC
		if i%5 == 0 {
			e := lst.PushFront(rand.Intn(seed))
			lst.Remove(e)
		}
	}

	// Sum values in the list
	var sum int64 = 0
	for e := lst.Front(); e != nil; e = e.Next() {
		if val, ok := e.Value.(int); ok {
			sum += int64(val)
		}
	}

	executionTime := time.Since(start).Microseconds()

	// Gather memory statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Fill the response structure
	response.Sum = sum
	response.ExecutionTime = executionTime
	response.RequestNumber = REQ_NUM
	response.ArraySize = ARRAY_SIZE
	response.HeapAllocMemory = m.HeapAlloc
	response.GOGC = os.Getenv("GOGC")
	response.GOMEMLIMIT = os.Getenv("GOMEMLIMIT")
	response.NextGC = m.NextGC
	response.NumGC = m.NumGC

	// Convert the response to a map for OpenWhisk
	responseMap := make(map[string]interface{})
	responseBytes, _ := json.Marshal(response)
	json.Unmarshal(responseBytes, &responseMap)

	return responseMap
}
