START FOR %%F IN (LIDAL20??-DOY0*.*) DO DeadTime.exe %%F > 0.log
START FOR %%G IN (LIDAL20??-DOY1*.*) DO DeadTime.exe %%G > 1.log
START FOR %%H IN (LIDAL20??-DOY2*.*) DO DeadTime.exe %%H > 2.log
START FOR %%I IN (LIDAL20??-DOY3*.*) DO DeadTime.exe %%I > 3.log