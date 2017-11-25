wget https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
rm get-pip.py

pip3 install -r requirements.txt

sudo apt-get install software-properties-common
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xF1656F24C74CD1D8
sudo add-apt-repository 'deb [arch=amd64,i386,ppc64el] http://sfo1.mirrors.digitalocean.com/mariadb/repo/10.2/ubuntu xenial main'

sudo apt update
sudo apt install -y mariadb-server
