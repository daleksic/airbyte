FROM alpine:3.4 AS seed

WORKDIR /

# the sole purpose of this image is to seed the data volume with the default data
# that the app should have when it is first installed.
COPY build/resources/main/config latest_seeds/config