@echo off
setlocal
set "REPO_ROOT=%~dp0..\.."
"%REPO_ROOT%\.venv-airflow\Scripts\python.exe" "%REPO_ROOT%\scripts\utils\run_airflow.py" %*
