apt-get update
apt-get install sqlite3

python -m venv ./.venv

pip install -r requirements.txt

mkdir ./db

python setup.py