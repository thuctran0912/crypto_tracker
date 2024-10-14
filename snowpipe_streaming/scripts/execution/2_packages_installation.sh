passwd=changeit  
directory=/home/crypto_project/snowpipe-streaming

cd $HOME
mkdir -p $directory
cd $directory
pwd=`pwd`
sudo yum clean all
sudo yum -y install openssl vim-common java-1.8.0-openjdk-devel.x86_64 gzip tar jq python3-pip
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar xvfz kafka_2.12-2.8.1.tgz -C $pwd
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.1/aws-msk-iam-auth-1.1.1-all.jar -O $pwd/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.1-all.jar
rm -rf $pwd/kafka_2.12-2.8.1.tgz
cd /tmp && cp /usr/lib/jvm/java-openjdk/jre/lib/security/cacerts kafka.client.truststore.jks
cd /tmp && keytool -genkey -keystore kafka.client.keystore.jks -validity 300 -storepass $passwd -keypass $passwd -dname "CN=snowflake.com" -alias snowflake -storetype pkcs12

#Snowflake kafka connector
wget https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/2.2.1/snowflake-kafka-connector-2.2.1.jar -O $pwd/kafka_2.12-2.8.1/libs/snowflake-kafka-connector-2.2.1.jar

#Snowpipe streaming SDK
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-ingest-sdk/2.1.0/snowflake-ingest-sdk-2.1.0.jar -O $pwd/kafka_2.12-2.8.1/libs/snowflake-ingest-sdk-2.1.0.jar
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.5/snowflake-jdbc-3.14.5.jar -O $pwd/kafka_2.12-2.8.1/libs/snowflake-jdbc-3.14.5.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar -O $pwd/kafka_2.12-2.8.1/libs/bc-fips-1.0.1.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar -O $pwd/kafka_2.12-2.8.1/libs/bcpkix-fips-1.0.3.jar