keytool -genkey -keystore sslKey
keytool -export -file srvcert.crt -keystore sslKey
keytool -import -file srvcert.crt -keystore sslTrust