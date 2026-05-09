FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder
LABEL maintainer="nekohasekai <contact-git@sekai.icu>"
COPY . /go/src/github.com/sagernet/sing-box
WORKDIR /go/src/github.com/sagernet/sing-box
ARG TARGETOS TARGETARCH
ARG GOPROXY=""
ENV GOPROXY ${GOPROXY}
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

# Install build dependencies
RUN set -ex \
    && apk add git build-base

# Conditionally install Rust and build the reticulum bridge for supported architectures
RUN set -ex \
    && if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then \
        apk add rust cargo; \
        cd bridge && cargo build --release && cd ..; \
    fi

# Build sing-box
RUN set -ex \
    && export COMMIT=$(git rev-parse --short HEAD) \
    && export VERSION=$(go run ./cmd/internal/read_tag) \
    && export TAGS=$(cat release/DEFAULT_BUILD_TAGS_OTHERS) \
    && export LDFLAGS_SHARED=$(cat release/LDFLAGS) \
    && if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then \
        CGO_ENABLED=1 \
        go build -v -trimpath -tags "$TAGS,with_reticulum" \
            -o /go/bin/sing-box \
            -ldflags "-X \"github.com/sagernet/sing-box/constant.Version=$VERSION\" $LDFLAGS_SHARED -s -w -buildid=" \
            ./cmd/sing-box; \
    else \
        CGO_ENABLED=0 \
        go build -v -trimpath -tags "$TAGS" \
            -o /go/bin/sing-box \
            -ldflags "-X \"github.com/sagernet/sing-box/constant.Version=$VERSION\" $LDFLAGS_SHARED -s -w -buildid=" \
            ./cmd/sing-box; \
    fi

FROM --platform=$TARGETPLATFORM alpine AS dist
LABEL maintainer="nekohasekai <contact-git@sekai.icu>"
RUN set -ex \
    && apk add --no-cache --upgrade bash tzdata ca-certificates nftables
COPY --from=builder /go/bin/sing-box /usr/local/bin/sing-box
ENTRYPOINT ["sing-box"]