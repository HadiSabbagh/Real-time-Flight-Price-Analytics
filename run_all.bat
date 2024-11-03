@echo off
cd /d C:\kafka_2.12-3.8.0\bin\windows

start  cmd /k zookeeper-server-start.bat ..\..\config\zookeeper.properties
timeout /t 5
start  cmd /k kafka-server-start.bat ..\..\config\server.properties
