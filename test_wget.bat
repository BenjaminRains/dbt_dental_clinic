@echo off
REM Output directory for the downloaded files
set OUTPUT_DIR=docs\opendental_manual

REM Create the output directory if it doesn't exist
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM Download all URLs from the list with verbose output
REM -c: continue partially downloaded files
REM -nc: no clobber (don't overwrite existing files)
REM -N: don't re-retrieve files unless newer than local
REM -v: verbose output
wget -i docs\urls.txt -P "%OUTPUT_DIR%" --no-check-certificate -E -H -k -p -nc -N -v

REM Check if files were downloaded
echo.
echo Checking downloaded files:
dir "%OUTPUT_DIR%"
