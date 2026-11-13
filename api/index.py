import sys
from pathlib import Path

# Añadir la raíz del repositorio a sys.path para importar app.py en el entorno serverless de Vercel
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import app
