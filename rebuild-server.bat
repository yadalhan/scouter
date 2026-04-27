@echo off
setlocal

echo [INFO] Setting up build environment...
set JAVA_HOME=C:\SW\jdk1.8.0_481
set PATH=%JAVA_HOME%\bin;%PATH%

set MAVEN_CMD=C:\Temp\ag-projects\maven\apache-maven-3.9.6\bin\mvn.cmd

echo [INFO] Rebuilding scouter.server module...
call %MAVEN_CMD% clean package -DskipTests -pl scouter.server -am

if %ERRORLEVEL% == 0 (
    echo [SUCCESS] scouter.server build completed successfully.
) else (
    echo [ERROR] scouter.server build failed.
)

endlocal
