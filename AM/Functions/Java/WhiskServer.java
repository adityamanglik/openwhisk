import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class WhiskServer {
    public static void main(String[] args) throws IOException {
        // Log Java version
        System.out.println("Running on Java: " + System.getProperty("java.version"));

        // Must listen on port 8080 for OpenWhisk
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        // Required: handle /run
        server.createContext("/run", exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    // Read request body
                    StringBuilder requestBody = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            requestBody.append(line);
                        }
                    }

                    // Parse JSON input
                    JsonObject input = JsonParser.parseString(requestBody.toString()).getAsJsonObject();

                    // Call business logic
                    JsonObject result = Hello.main(input);

                    // Send JSON response
                    String response = result.toString();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, response.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    String errorMessage = "{\"error\":\"Internal server error\",\"details\":\"" + e.getMessage() + "\"}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(500, errorMessage.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMessage.getBytes());
                    }
                }
            } else {
                exchange.sendResponseHeaders(405, 0); // Method Not Allowed
                exchange.close();
            }
        });

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down HTTP server...");
            server.stop(0);
        }));

        // Start server
        server.start();
        System.out.println("HTTP server started on port 8080");
    }
}
