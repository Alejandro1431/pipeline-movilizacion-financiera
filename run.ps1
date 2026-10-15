# Arranque rápido en Windows: crea el entorno, instala y corre el pipeline.
#   .\run.ps1            -> pipeline completo
#   .\run.ps1 test       -> solo las pruebas
#   .\run.ps1 quality    -> solo las reglas de calidad

param([string]$Tarea = 'all')

$ErrorActionPreference = 'Stop'
# La consola de Windows no usa UTF-8 por defecto: sin esto los acentos salen rotos.
$env:PYTHONIOENCODING = 'utf-8'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$raiz = $PSScriptRoot
$venv = Join-Path $raiz '.venv'
$python = Join-Path $venv 'Scripts\python.exe'

if (-not (Test-Path $python)) {
    Write-Host 'Creando entorno virtual...' -ForegroundColor Cyan
    python -m venv $venv
    & $python -m pip install --quiet --upgrade pip
    & $python -m pip install --quiet -r (Join-Path $raiz 'requirements.txt')
}

$env:PYTHONPATH = Join-Path $raiz 'src'

switch ($Tarea) {
    'test'    { & $python -m pytest -q }
    'extract' { & $python -m pipeline extract }
    'quality' { & $python -m pipeline quality }
    default   { & $python -m pipeline all }
}

exit $LASTEXITCODE
