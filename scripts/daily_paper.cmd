@echo off
rem ============================================================
rem  Daily paper pipeline: live scan -> research scan -> paper replays
rem  Scheduled task: "sol-signal-paper-daily" (23:50 daily)
rem  Note: keep this file ASCII-only. cmd.exe parses .cmd using the
rem  system ANSI codepage, so non-ASCII text here corrupts the script.
rem ============================================================
setlocal
cd /d "%~dp0.."
chcp 65001 >nul 2>&1
set "PYTHONIOENCODING=utf-8"
set "PY=python"
where python >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python not found: %PY%
  exit /b 1
)
if not exist "logs\paper\daily" mkdir "logs\paper\daily"
set "LOG=logs\paper\daily\daily.log"
echo. >> "%LOG%"
echo ================== %DATE% %TIME% start ================== >> "%LOG%"
call run.cmd --once --no-toast >> "%LOG%" 2>&1
echo [%TIME%] live scan done >> "%LOG%"
"%PY%" scripts\research_scan.py --once --write >> "%LOG%" 2>&1
echo [%TIME%] research scan done >> "%LOG%"
"%PY%" scripts\paper_runner.py --once --write >> "%LOG%" 2>&1
echo [%TIME%] paper replay live done >> "%LOG%"
"%PY%" scripts\paper_runner.py --once --write --signals research >> "%LOG%" 2>&1
echo [%TIME%] paper replay research done >> "%LOG%"
echo ================== %DATE% %TIME% end ================== >> "%LOG%"
exit /b 0
