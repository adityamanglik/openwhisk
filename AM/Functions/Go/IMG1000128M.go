package main

import (
	"encoding/json"
	"image"
	"image/color"
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

var cachedImage image.Image

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

func preloadImage() {
	fileNames := []string{"Resources/img1.jpg", "Resources/img2.jpg"}
	selectedFile := fileNames[rand.Intn(len(fileNames))]

	file, err := os.Open(selectedFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	img, _, err := image.Decode(file)
	if err != nil {
		panic(err)
	}

	cachedImage = img
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

	start := time.Now().UnixMicro()
	rand.Seed(int64(seed))

	img := cachedImage
	bounds := img.Bounds()

	// Resize the image
	newImg := resize(img, ARRAY_SIZE)

	// Sum all pixel values
	sum := sumPixels(newImg)

	// Create a random source and a new random generator
	randomSrc := rand.NewSource(int64(seed))
	rnd := rand.New(randomSrc)

	// Add random seed to every pixel
	for y := bounds.Min.Y; y < bounds.Max.Y/16; y++ {
		for x := bounds.Min.X; x < bounds.Max.X/16; x++ {
			originalColor := color.RGBAModel.Convert(img.At(x, y)).(color.RGBA)
			r := clamp(uint32(originalColor.R) + uint32(rnd.Intn(256)))
			g := clamp(uint32(originalColor.G) + uint32(rnd.Intn(256)))
			b := clamp(uint32(originalColor.B) + uint32(rnd.Intn(256)))
			newColor := color.RGBA{uint8(r), uint8(g), uint8(b), originalColor.A}
			newImg.Set(x, y, newColor)
		}
	}
	sum += sumPixels(newImg)

	// Flip the image horizontally
	newImg = flipHorizontally(newImg)
	sum += sumPixels(newImg)

	// Rotate the image by 90 degrees
	newImg = rotate(newImg, 90)
	sum += sumPixels(newImg)

	executionTime := time.Now().UnixMicro() - start

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

// Implement a basic nearest-neighbor resizing algorithm
func resize(img image.Image, newSize int) *image.RGBA {
	srcBounds := img.Bounds()
	dstBounds := image.Rect(0, 0, newSize, newSize)
	newImg := image.NewRGBA(dstBounds)

	xRatio := float64(srcBounds.Dx()) / float64(newSize)
	yRatio := float64(srcBounds.Dy()) / float64(newSize)

	for y := 0; y < newSize; y++ {
		for x := 0; x < newSize; x++ {
			srcX := int(float64(x) * xRatio)
			srcY := int(float64(y) * yRatio)
			newImg.Set(x, y, img.At(srcX, srcY))
		}
	}

	return newImg
}

func sumPixels(img image.Image) int64 {
	var sum int64 = 0
	bounds := img.Bounds()
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, _ := img.At(x, y).RGBA()
			sum += int64(r + g + b)
		}
	}
	return sum
}

func flipHorizontally(img image.Image) *image.RGBA {
	bounds := img.Bounds()
	flipped := image.NewRGBA(bounds)
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			flipped.Set(bounds.Max.X-x, y, img.At(x, y))
		}
	}
	return flipped
}

func rotate(img image.Image, angle int) *image.RGBA {
	bounds := img.Bounds()
	rotated := image.NewRGBA(bounds)
	for y := 0; y < bounds.Dy(); y++ {
		for x := 0; x < bounds.Dx(); x++ {
			rotated.Set(bounds.Dx()-x-1, y, img.At(x, y))
		}
	}
	return rotated
}

func clamp(value uint32) uint32 {
	if value > 255 {
		return 255
	}
	return value
}
