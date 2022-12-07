#!/bin/bash

SOURCE=$1;
TARGET=$2;

for val in $(cat $SOURCE)
do
	LINE=$val
	echo "reading line:" $LINE
	
	KEY=${LINE%%=*}
	VALUE=${LINE#*=}
	START_WITH="^${KEY}"
	EXISTS=`grep "$KEY=" $TARGET | grep -v grep`
	echo "	KEY" $KEY
	echo "	VALUE" $VALUE
	echo "	EXISTS" $EXISTS

	if [ -n "$EXISTS" ]; then
		VALUE2=${EXISTS#*=}
		if [[ "$OSTYPE" == "darwin"* ]]; then
			sed -i '' 's+'$VALUE2'+'$VALUE'+g' $TARGET
		else
        	sed -i 's+'$VALUE2'+'$VALUE'+g' $TARGET
		fi
	fi
done
