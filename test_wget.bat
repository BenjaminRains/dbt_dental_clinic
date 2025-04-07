@echo off
REM Output directory for the downloaded files
set OUTPUT_DIR=docs\opendental_manual

REM Create the output directory if it doesn't exist
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

REM Download all URLs from the list
wget -i docs\urls.txt -P "%OUTPUT_DIR%" --no-check-certificate -E -H -k -p -nc
