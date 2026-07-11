# 1. Use a lightweight Java 21 runtime as the base OS
FROM eclipse-temurin:21-jre-alpine

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy our compiled Fat JAR from the host machine into the container
COPY target/kafka-lite-storage-1.0-SNAPSHOT.jar /app/kafka-lite.jar

# 4. Create a data directory inside the container for our .log files
RUN mkdir -p /app/data

# 5. Expose our custom Netty TCP port to the outside world
EXPOSE 9092

# 6. The command that runs when the container starts
ENTRYPOINT ["java", "-jar", "kafka-lite.jar"]