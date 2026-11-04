@echo off
setlocal
chcp 65001 >nul 2>&1
set "PYTHONIOENCODING=utf-8"
set "DEMO_PY=python"
where python >nul 2>&1
if errorlevel 1 (
  echo Python runtime was not found.
  pause
  exit /b 1
)
echo OFFLINE SEMI-AUTOMATIC DEMO - no exchange orders are sent.
"%DEMO_PY%" -X utf8 -B "%~dp0scripts\semi_auto.py" --interactive-demo --log-root "%~dp0logs\interactive-user"
set "DEMO_EXIT=%ERRORLEVEL%"
echo.
echo Demo finished. Results are saved in logs\interactive-user.
pause
exit /b %DEMO_EXIT%
