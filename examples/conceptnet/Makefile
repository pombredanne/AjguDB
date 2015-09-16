all:
	@echo "try make load"

clean:
	rm -rf db

download:
	wget http://conceptnet5.media.mit.edu/downloads/current/conceptnet5_db_5.4.tar.bz2

unpack:
	tar xvf conceptnet5_db_5.4.tar.bz2
	rm conceptnet5_db_5.4.tar.bz2

load: download unpack
	python load.py
