const ARRAY_SIZE = 100;
function generateRandomNormal(mean, stdDev) {
  // Box-Muller transform to generate a random number from a normal distribution
  const u1 = Math.random();
  const u2 = Math.random();
  const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  return z0 * stdDev + mean;
}

function main(params) {
  // If seed is not provided in params, default to 42
  const seedValue = params.seed ? params.seed : 42;

  // Create an array
  const randomNumbers = [];

  // Write random numbers into the array up to ARRAY_SIZE
  for (let i = 0; i < ARRAY_SIZE; i++) {
    const randomNum = Math.random() * seedValue;
    randomNumbers.push(randomNum);
  }

  // Calculate the sum of the array
  const arraySum = randomNumbers.reduce((acc, val) => acc + val, 0);

  // Calculate size of heap
  const { performance } = require('perf_hooks');
  const usedHeapSize = process.memoryUsage().heapUsed;
  const totalHeapSize = process.memoryUsage().heapTotal;
  // Again, Node.js doesn't provide a direct equivalent to jsHeapSizeLimit, but you can get the resident set size
  const residentSetSize = process.memoryUsage().rss;

  // return { payload: `Seed: ${seedValue}\nThe sum of the array values is ${arraySum}` };
  return { 
    payload: `usedHeapSize: ${usedHeapSize} ` +
             `totalHeapSize: ${totalHeapSize} ` +
             `HeapSizeLimit: ${residentSetSize} ` +
             `The sum of the array values is ${arraySum}`
};


}

// For testing purposes
