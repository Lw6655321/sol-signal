@echo off
setlocal
cd /d "%~dp0"
python -X utf8 -B -m src.main %*
exit /b %ERRORLEVEL%
