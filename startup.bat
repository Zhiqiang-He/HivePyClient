mode con cols=120 lines=30
set PYTHOME_HOME=C:\Python27
set PATH=%PYTHOME_HOME%;%PATH%
set PYTHONPATH=%PYTHONPATH%;%~dp0
echo will excute cmd: 'python %~dp0\pytools\HiveClient.py'
python "%~dp0\pytools\PyConsole.py"