#!/bin/sh
echo "Running unit test ..."
export XE_MYSQL_HOST=$(echo $1 | rev | cut -d'/' -f 1 | rev)
export XE_MYSQL_DBNAME=$(echo $2 | rev | cut -d'/' -f 1 | rev)
export XE_MYSQL_USER=$(echo $3 | rev | cut -d'/' -f 1 | rev)
export XE_MYSQL_PASSWORD=$(echo $4 | rev | cut -d'/' -f 1 | rev)

python -m unittest unit_tests
if [ $? = 0 ]
then
  python xe_consume.py
else
  echo "UNIT TEST FAILED"
fi
