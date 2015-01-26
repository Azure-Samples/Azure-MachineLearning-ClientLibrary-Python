@echo OFF
SETLOCAL
cls

if "%1%" == "" (
	set PYTHONDIR=%SystemDrive%\Anaconda
) else (
	set PYTHONDIR=%1%
)

if "%2%" == "" (
	set COVERAGEDIR=htmlcov
) else (
	set COVERAGEDIR=%2%
)

if "%PYTHONPATH%" == "" (
	set PYTHONPATH=..
) else (
	set PYTHONPATH=%PYTHONPATH%;..
)

if exist "%PYTHONDIR%\Scripts\coverage.exe" (
	goto :coverage
)


REM ---------------------------------------------------------------------------
if not exist "%PYTHONDIR%\Scripts\pip.exe" (
	echo Cannot do a code coverage run when neither 'coverage' nor 'pip' are installed.
	goto :exit_door
)

echo Installing 'coverage' package...
%PYTHONDIR%\Scripts\pip.exe install coverage
echo Finished installing 'coverage' package

REM ---------------------------------------------------------------------------
:coverage
echo Starting coverage run using %PYTHONDIR%
%PYTHONDIR%\Scripts\coverage.exe run -m unittest discover -p "unittests.py"
%PYTHONDIR%\Scripts\coverage.exe html -d %COVERAGEDIR%
start %CD%\%COVERAGEDIR%\index.html
echo Finished coverage run!

REM ---------------------------------------------------------------------------
:exit_door
exit /B %UNITTEST_EC%