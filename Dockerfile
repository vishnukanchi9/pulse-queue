FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src src
# Package only. The integration tests need a Docker daemon to start PostgreSQL
# and Redis, and there is no socket inside an image build - running them here
# would mean nesting Docker in Docker to re-run what CI already ran one step
# earlier. The pipeline tests; the image build packages.
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/target/pulse-queue-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
