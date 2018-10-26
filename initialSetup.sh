#! /usr/bin/env bash
sudo yum upgrade
sudo yum update
sudo yum install git
sudo add-apt-repository ppa:webupd8team/java
sudo yum update
sudo yum install oracle-java8-installer
sudo yum install maven
sudo update-alternatives --config java
sudo yum install wget
sudo yum install git
sudo yum groupinstall 'Development Tools'
#source /etc/environment
echo $JAVA_HOME >> /etc/environment

sudo yum install make
sudo yum install gcc
sudo yum install tcl
sudo yum install build-essential

sudo add-apt-repository ppa:deadsnakes/ppa
sudo yum update
sudo yum install python2.7

wget -O- https://raw.githubusercontent.com/nicolargo/glancesautoinstall/master/install.sh | sudo /bin/bash


wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
sudo mkdir -p /usr/local/bin/
sudo mv ./lein* /usr/local/bin/lein
sudo chmod a+x /usr/local/bin/lein
export PATH=$PATH:/usr/local/bin
lein repl
sudo reboot


