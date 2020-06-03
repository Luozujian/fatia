#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/15 15:35
# @Author  : luozujian

import matlab
import matlab.engine
class MatlabTest:

    def __init__(self):
        self.eng = matlab.engine.start_matlab()
        pass

    def getVoteResult(self):
        result = self.eng.predictFunction()
        return result

    def getRevenueResult(self):
        result = self.eng.revenuePredictFunction()
        return result

    def getTvResult(self):
        result = self.eng.tvVotePredictFunction()
        return result


if __name__ == '__main__':

    app = MatlabTest()
    print(app.getResult())
#D:\pythonproject\bishe\Scripts\python.exe
#%JAVA_HOME%\bin;%JAVA_HOME%\jre\bin;E:\RM\ARm\bin;E:\program\ARM\bin;D:\armdeveloper_jb51 (1)\ARm\bin;D:\ARM\bin;C:\ProgramData\Oracle\Java\javapath;%SystemRoot%\system32;%SystemRoot%;%SystemRoot%\System32\Wbem;%SYSTEMROOT%\System32\WindowsPowerShell\v1.0\;C:\Program Files\MySQL\MySQL Utilities 1.6\;D:\Program\matlab2018b\app\runtime\win64;D:\Program\matlab2018b\app\bin;D:\Program\matbla\runtime\win64;D:\Program\matbla\bin;D:\Program\matbla\polyspace\bin;D:\wamp\bin\mysql\mysql5.6.12\bin;D:\Program\node-v10.1.0-win-x64;D:\Program\Git\cmd;D:\pythonproject\bishe\Scripts;D:\redis-3.0\redis-3.0;D:\Redis\;D:\Git\cmd;%MAVEN_HOME%\bin;%ERLANG_HOME%\bin;D:\Program\Nodejs\;D:\Program\ffmpeg-20180227-fa0c9d6-win64-static\bin;%GRADLE_HOME%\bin