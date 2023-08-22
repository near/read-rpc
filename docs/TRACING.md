# Enabling Tracing with Jaeger for ReadRPC

## Introduction

Tracing is an essential aspect of distributed systems that allows developers to monitor and analyze the flow of requests and responses across various components. This document explains how to enable tracing in ReadRPC, using Jaeger as the tracing backend.

## Prerequisites

Before proceeding with enabling tracing, make sure you have the following prerequisites:

1. (Optionally) Docker and Docker Compose installed on your system.
2. A running instance of Jaeger to which the tracing data will be exported. Ensure you have the Jaeger URL ready.

## Enabling Tracing

ReadRPC provides a feature called `tracing-instrumentation`, which enables tracing compatibility with `opentelemetry`, and we recommend using Jaeger as the tracing backend.

To enable tracing, follow these steps:

1. **Set Up Jaeger**

In the `docker-compose.yml` file provided with ReadRPC, Jaeger is already set up and configured for you. This means you can use Jaeger out of the box without having to install or configure it separately.

2. **Export Jaeger Endpoint**

To connect ReadRPC with your instance of Jaeger, you need to provide an environmental variable `OTEL_EXPORTER_JAEGER_ENDPOINT` pointing to the Jaeger's trace API endpoint. Make sure you have the URL of your Jaeger instance ready before proceeding.

In your ReadRPC's environment setup (e.g., `.env` file or similar), add the following line:

```
OTEL_EXPORTER_JAEGER_ENDPOINT=http://your-jaeger-host/api/traces
```

Replace `your-jaeger-host` with the actual URL of your Jaeger instance.

3. **Enable Tracing Instrumentation**

With the environmental variable set, ReadRPC's `tracing-instrumentation` feature will now automatically export traces to your Jaeger instance.

## Using Jaeger UI

To view the traces collected by Jaeger, you can access the Jaeger UI using the following URL:

```
http://your-jaeger-host:16686
```

Replace `your-jaeger-host` with the actual URL of your Jaeger instance.

Once you access the Jaeger UI, you will be able to explore and analyze the traces generated by ReadRPC's components.

## Conclusion

Enabling tracing in ReadRPC is as simple as enabling the `tracing-instrumentation` feature and providing the appropriate Jaeger endpoint URL. With Jaeger as the tracing backend, you can gain valuable insights into the behavior of your distributed system and troubleshoot issues effectively.

If you encounter any issues or have any questions, please feel free to reach out to us on ReadRPC's communication channels.

Happy tracing!