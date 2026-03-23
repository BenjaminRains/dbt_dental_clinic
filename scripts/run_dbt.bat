@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0ec2\run_dbt_on_ec2.ps1" %*
