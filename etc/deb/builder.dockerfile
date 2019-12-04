FROM debian:stretch as builder

# Install the system dependencies
# Attempt to clean and rebuild the cache to avoid 404s
RUN apt update && \
    apt install -y fakeroot
