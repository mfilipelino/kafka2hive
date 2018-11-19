
install:
	virtualenv -p python2.7 venv
	venv/bin/python venv/bin/pip install -r requirements.txt

clean:
	rm -rf venv

test:
	hive -f sql/drop_table.sql
	hive -f sql/create_table.sql