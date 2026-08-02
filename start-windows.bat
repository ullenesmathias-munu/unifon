@echo off
title Unifon Capture Server

:start
echo Starting Unifon queue-presence capture...
node capture-server.js

echo.
echo capture-server.js stopped (exit code %errorlevel%).
echo Restarting in 10 seconds - press Ctrl+C now to stop instead.
ping -n 11 127.0.0.1 >nul
goto start
