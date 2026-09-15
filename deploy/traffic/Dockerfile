FROM alpine:3.22 AS fetch
ARG XRAY_VERSION=26.7.11
ARG XRAY_SHA256=aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e
RUN apk add --no-cache curl unzip \
 && curl --fail --location --retry 4 --output /tmp/xray.zip \
      "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
 && echo "${XRAY_SHA256}  /tmp/xray.zip" | sha256sum -c - \
 && mkdir /out \
 && unzip -q /tmp/xray.zip -d /out \
 && /out/xray version

FROM alpine:3.22
RUN addgroup -S soty && adduser -S -G soty -h /var/lib/soty-traffic soty
COPY --from=fetch /out/xray /usr/local/bin/xray
COPY --from=fetch /out/geoip.dat /usr/local/share/xray/geoip.dat
COPY --from=fetch /out/geosite.dat /usr/local/share/xray/geosite.dat
USER soty
ENTRYPOINT ["/usr/local/bin/xray"]
CMD ["run", "-c", "/etc/soty-traffic/server.json"]
