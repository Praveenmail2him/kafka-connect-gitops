FROM registry.access.redhat.com/ubi9/openjdk-17-runtime:latest

WORKDIR /deployments
COPY target/orders-service.jar /deployments/app.jar

EXPOSE 8080
USER 185

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/deployments/app.jar"]
