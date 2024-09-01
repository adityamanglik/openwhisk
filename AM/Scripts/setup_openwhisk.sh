# Download wsk
cd ~
wget https://github.com/apache/openwhisk-cli/releases/download/latest/OpenWhisk_CLI-latest-linux-amd64.tgz
tar -zxvf OpenWhisk_CLI-latest-linux-amd64.tgz
chmod +x ./wsk
sudo mv wsk /usr/bin/

# Build openwhisk from source and enable API
git clone https://github.com/adityamanglik/openwhisk.git
git clone https://github.com/adityamanglik/openwhisk-devtools.git

cd openwhisk
# Copy standalone config file
cp /users/am_CU/openwhisk-devtools/docker-compose/Scripts/standalone.conf /users/am_CU/openwhisk/core/standalone/src/main/resources/standalone.conf
#./gradlew core:standalone:bootRun --args='--api-gw'
./gradlew core:standalone:bootRun --args="--api-gw -c /users/am_CU/openwhisk/core/standalone/src/main/resources/standalone.conf"
source openwhisk_action_setup.sh
