# Use Amazon Corretto as the base
FROM amazoncorretto:17

# Create a work directory
WORKDIR /app

# Copy all source files into container
COPY . /app/

# Compile your Java code with classpath referencing gson
RUN javac -cp .:gson-2.10.1.jar Hello.java WhiskServer.java

# Expose port 8080 for OpenWhisk to call
EXPOSE 8080

# Launch the server
CMD java -cp .:gson-2.10.1.jar -XX:+UseSerialGC -Xms128m -Xmx128m WhiskServer