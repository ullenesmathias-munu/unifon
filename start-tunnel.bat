@echo off
title Unifon Cloudflare Tunnel
echo This exposes http://localhost:8787 to the internet via a Cloudflare quick tunnel.
echo Watch for a line below like:  https://random-words.trycloudflare.com
echo That URL is what you paste into the dashboard's "Capture source" field.
echo.

:start
cloudflared tunnel --url http://localhost:8787

echo.
echo cloudflared stopped (exit code %errorlevel%).
echo Restarting in 10 seconds - press Ctrl+C now to stop instead.
echo NOTE: restarting gives you a NEW trycloudflare.com URL - update the dashboard when this happens.
ping -n 11 127.0.0.1 >nul
goto start
