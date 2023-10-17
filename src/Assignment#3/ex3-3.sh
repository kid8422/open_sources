#!/bin/sh

weight=$1
height=$2

height_squared=$(($height * $height))
bmi=$(($weight * 100000 / $height_squared))

if [ $bmi -le 185 ]; then
    echo "저체중입니다."
elif [ $bmi -lt 230 ]; then
    echo "정상체중입니다."
else
    echo "과체중입니다."
fi

exit 0