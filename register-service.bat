@echo on

@rem C:\Windows\Microsoft.NET\Framework64\v4.0.30319\InstallUtil.exe /ServiceName="ESB-Server" ESBServer.exe

sc create "Paragonex ESB Server" binPath= "C:\DriveD\Services\ESBServer\ESBServer.exe"