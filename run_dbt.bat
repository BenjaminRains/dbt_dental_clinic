@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0run_dbt.ps1" %*
