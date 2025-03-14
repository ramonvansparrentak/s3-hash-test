FROM eclipse-temurin:17 AS build

COPY src /project/src
COPY gradle /project/gradle
COPY gradlew /project
COPY settings.gradle /project
COPY build.gradle /project

WORKDIR /project
RUN ls
RUN ./gradlew wrapper
RUN ./gradlew jar

FROM eclipse-temurin:17

COPY --from=build /project/build/libs/s3-hash-test-1.0-SNAPSHOT.jar /app.jar

ENTRYPOINT ["java", "-jar", "/app.jar" ]