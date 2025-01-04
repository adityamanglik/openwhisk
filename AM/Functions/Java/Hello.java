import com.google.gson.JsonObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.GarbageCollectorMXBean;

public class Hello {

    public static JsonObject main(JsonObject args) {
        // Generate a non-deterministic seed (use current time in nanoseconds)
        long seed = System.nanoTime();
        int ARRAY_SIZE = 10000; // default arraysize value
        int request_number = Integer.MAX_VALUE; // default arraysize value
        if (args.has("seed")) {
            seed = args.getAsJsonPrimitive("seed").getAsInt();
        }

        if (args.has("arraysize")) {
            ARRAY_SIZE = args.getAsJsonPrimitive("arraysize").getAsInt();
        }

        if (args.has("requestnumber")) {
            request_number = args.getAsJsonPrimitive("requestnumber").getAsInt();
        }

        long startTime = System.nanoTime(); // Start time tracking

        Random rand = new Random(seed);
        
        List<Object> lst = new LinkedList<>();

        for (int i = 0; i < ARRAY_SIZE; i++) {
            // Direct insertion to stress GC
            lst.add(0, rand.nextInt());

            // Nested lists to create more objects and stress GC
            if (i % 5 == 0) {
                List<Object> nestedList = new LinkedList<>();
                for (int j = 0; j < rand.nextInt(5); j++) {
                    nestedList.add(rand.nextInt());
                }
                lst.add(nestedList);
            }

            // Immediate removal after insertion to stress GC
            if (i % 5 == 0) {
                lst.add(0, rand.nextInt());
                lst.remove(0);
            }
        }

        // Sum values to mimic computation
        long sum = 0; // use long to avoid integer overflow if you expect the sum to be large
        for (Object obj : lst) {
            if (obj instanceof Integer) {
                sum += (Integer) obj;
            }
            // If there are nested lists, you would need to handle them as well
            else if (obj instanceof List) {
                for (Object nestedObj : (List) obj) {
                    if (nestedObj instanceof Integer) {
                        sum += (Integer) nestedObj;
                    }
                }
            }
        }

        long executionTime = System.nanoTime() - startTime; // Calculate execution time
        executionTime = executionTime/1000;
        JsonObject response = new JsonObject();
        
        // Garbage collector information
        int gcIndex = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcIndex++; // To distinguish between different GC objects
            long count = gc.getCollectionCount();
            if (count != -1) { // -1 if the collection count is not available
                response.addProperty("gc" + gcIndex + "CollectionCount", count);
            }
            long timer = gc.getCollectionTime();
            if (timer != -1) { // -1 if the collection time is not available
                response.addProperty("gc" + gcIndex + "CollectionTime", timer);
            }
        }

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        long initMemory = heapMemoryUsage.getInit();  // Initial memory amount
        long usedMemory = heapMemoryUsage.getUsed();  // Amount of used memory
        long committedMemory = heapMemoryUsage.getCommitted(); // Amount of memory guaranteed to be available for the JVM
        long maxMemory = heapMemoryUsage.getMax();   // Maximum amount of memory (can change over time, can be undefined)
        response.addProperty("heapInitMemory", initMemory);
        response.addProperty("heapUsedMemory", usedMemory);
        response.addProperty("heapCommittedMemory", committedMemory);
        response.addProperty("heapMaxMemory", maxMemory);

        response.addProperty("sum", sum);
        response.addProperty("seed", seed);
        response.addProperty("arraysize", ARRAY_SIZE);
        response.addProperty("request", request_number);
        response.addProperty("executionTime", executionTime); // Add execution time to response

        
        return response;
    }

}
