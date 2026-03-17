@echo off
echo ======================================================
echo   GITHUB PUSH - Lavoro_Giupponi6
echo ======================================================
echo.
echo Sto per inviare il codice a:
echo https://github.com/businessxsrl-droid/Lavoro_giupponi6.git
echo.
echo Se appare una finestra del browser, effettua il login
echo con l'account 'businessxsrl-droid'.
echo.
pause
git push -u origin main
echo.
echo.
if %errorlevel% neq 0 (
    echo [ERRORE] Il push e' fallito. Verifica i permessi o il login.
) else (
    echo [OK] Codice inviato con successo!
)
pause
