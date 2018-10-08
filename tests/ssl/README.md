How to Generate .PEM and .JKS Files
-----------------------------------

* Make sure that you have OpenSSL installed on your system.

* Run the following command to generate your self-signed certificate and private key.

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 3650
```
* You can specify the validity period of your certificate with -days flag. 
You can set it to negative values to generate expired certificates.

* This command will ask you to enter a password for your private key. 
You should always encrypt your private key with a password, but you can omit this step
by adding -nodes flag.

```bash
openssl req -nodes -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 3650
```

* Then, you will be asked by OpenSSL to incorporate some information into your certificate.
You can leave most of them blank but be careful about the Common Name. That field will
be checked against your server's hostname.

* To generate .jks KeyStore/TrustStore from these files you need to combine them.

```bash
cat cert.pem key.pem > combined.pem
```

* Then, generate the PKCS #12 file from this combined certificate.

```bash
openssl pkcs12 -export -in combined.pem -out cert.p12
```

* Finally, generate the JKS file.

```bash
keytool -importkeystore -srckeystore cert.p12 -srcstoretype pkcs12 -destkeystore cert.jks
```
