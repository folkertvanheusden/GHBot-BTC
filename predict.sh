#! /bin/sh

cd /home/ghbot/btc
./predict-day-btc.py
scp -C btc-prediction.svg folkert@navi.vm.nurd.space:public_html/
