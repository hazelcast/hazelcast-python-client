#!/bin/bash

# Slightly modified version of 
# hazelcast-enterprise/src/test/resources/com/hazelcast/client/nio/ssl/createKeyMaterialForSanTests.sh

KEYSTORE_PASSWORD=123456

function createCertificates
{
  # Create keystores for the member
  if [ $# -gt 2 ]; then
    keytool -genkeypair -keystore "$1.p12" \
      -storepass $KEYSTORE_PASSWORD -keypass $KEYSTORE_PASSWORD \
      -storetype PKCS12 -validity 7300 -keyalg RSA -keysize 4096 \
      -alias test -dname "$2" -ext "$3"
  else
    keytool -genkeypair -keystore "$1.p12" \
      -storepass $KEYSTORE_PASSWORD -keypass $KEYSTORE_PASSWORD \
      -storetype PKCS12 -validity 7300 -keyalg RSA -keysize 4096 \
      -alias test -dname "$2"
  fi

  # Extract the cert in PEM format to be used as a truststore in the client
  openssl pkcs12 -in "$1.p12" -out "$1.pem" \
    -passin "pass:$KEYSTORE_PASSWORD" -nokeys
}

createCertificates tls-host-loopback-san "O=Hazelcast" "SAN=ip:127.0.0.1,ip:::1,dns:localhost,dns:localhost.localdomain"
createCertificates tls-host-loopback-san-dns "O=Hazelcast" "SAN=dns:localhost"
createCertificates tls-host-not-our-san "cn=public-dns" "SAN=ip:8.8.8.8"
createCertificates tls-host-loopback-cn "cn=localhost" "SAN=email:info@hazelcast.com"
createCertificates tls-host-no-entry "O=Hazelcast"
