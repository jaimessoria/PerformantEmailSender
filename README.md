# Run Application

Create Python virtual environment

pyenv global 3.7.4
Sudo pip3 install virtualenv
virtualenv .env
source .env/bin/activate

install requirements:  

pip3 install -r requirements.txt

Execute

python  PerformantEmailSender.py --dbfile db/db_file.csv 