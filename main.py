import sys
from middleware import Middleware

# El path del fichero de configuración debe ser bien conocido por el programa desde el inicio!
# INI_FILE_PATH = "/home/dcl/Desktop/PLOME/Middleware/plome.ini"
# INI_FILE_PATH = "/home/alex/Escritorio/PLOME/Middleware/plome.ini"

# Testing commit

if __name__ == "__main__":
    ini_file_path = sys.argv[1]
    print(ini_file_path)
    middleware = Middleware(ini_file_path)
    middleware.start()
