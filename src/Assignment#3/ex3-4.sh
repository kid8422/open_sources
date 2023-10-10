#!/bin/sh

echo "리눅스가 재밌나요? (yes / no)"

read answer

case $answer in
    yes | y | Y | Yes | YES | yyy | yesyesyes | 네 | 예 )
        echo "yes";;
    no | n | N | No | NO | nono | nonono | 아니요 | ㅠ | 아뇨)
        echo "no";;
    *)
        echo "yes or no로 입력해 주세요. "
        exit 1;;
esac

exit 0
