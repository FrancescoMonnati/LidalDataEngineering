
@echo off

rem Cancella tutti i file .bak dalla cartella K:\BackupDB
echo Eliminazione di tutti i file .bak nella cartella K:\BackupDB...
del /Q "K:\BackupDB\*.bak"
if %errorlevel% equ 0 (
    echo File .bak eliminati con successo dalla cartella K:\BackupDB.
) else (
    echo Si è verificato un errore durante l'eliminazione dei file .bak dalla cartella K:\BackupDB.
)

rem Definisci le variabili per le cartelle sorgente e destinazione
set source_folder=H:\
set destination_folder=K:\BackupDB

rem Verifica se ci sono file .bak nella cartella sorgente
echo Controllo dell'esistenza di file .bak nella cartella %source_folder%...
if exist "%source_folder%*.bak" (
    rem Copia tutti i file .bak dalla cartella sorgente alla cartella destinazione
    echo Copia dei file .bak dalla cartella %source_folder% alla cartella %destination_folder%...
    copy /Y "%source_folder%*.bak" "%destination_folder%"
    if %errorlevel% equ 0 (
        echo File .bak copiati con successo.
        
        rem Cancella i file .bak dalla cartella sorgente
        echo Eliminazione dei file .bak dalla cartella %source_folder%...
        del /Q "%source_folder%*.bak"
        if %errorlevel% equ 0 (
            echo File .bak eliminati con successo dalla cartella %source_folder%.
        ) else (
            echo Si è verificato un errore durante l'eliminazione dei file .bak dalla cartella %source_folder%.
        )
    ) else (
        echo Si è verificato un errore durante la copia dei file .bak.
    )
) else (
    echo Nessun file .bak trovato nella cartella %source_folder%.
)

pause
