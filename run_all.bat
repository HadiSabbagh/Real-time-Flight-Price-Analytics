@echo off
cd /d C:\data_analytics\kafka\bin\windows

start  cmd /k zookeeper-server-start.bat ..\..\config\zookeeper.properties
timeout /t 20
start  cmd /k kafka-server-start.bat ..\..\config\server.properties

