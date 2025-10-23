FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn package

FROM openjdk:17-jdk-slim

COPY --from=build /app/target/*.jar /app.jar

CMD ["java", "-jar", "/app.jar"]
