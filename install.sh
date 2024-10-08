#!/usr/bin/env bash

ssh_username="root"
ssh_password="couchbase"

usage() {
    echo "Params:"
    echo " -n   nodes/ip list"
    echo " -f   <file_name>.deb"
    echo " --help       Displays this help message"
    echo ""
}

install_cb_build() {
    echo "$1: Starting installation"
    sshpass -p $ssh_password ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $ssh_username@$node -t "$install_cmd" > /dev/null
    echo "$1: Installation complete"
}

while [ $# -ne 0 ]; do
    if [ "$1" == "-f" ]; then
        file_name=$2
        shift ; shift
    elif [ "$1" == "-n" ]; then
        nodes=$2
        download_only=false
        shift ; shift
done

if [ "$file_name" = "" ] || [ "$nodes" = "" ]; then
    usage
    exit 0
fi

# Previous version cleanup
service couchbase-server stop
apt-get remove -y "couchbase-server"
rm -rf /opt/couchbase

# Install Couchbase Server
dpkg -i $file_name
service couchbase-server start
