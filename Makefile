setup: requirements.txt
	pip install -r requirements.txt
	echo "Install and setup virtualenv"
	python3 -m pip install --upgrade pip
	python3 -m pip install virtualenv
	virtualenv $(VIRTUAL_ENV)