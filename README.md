# LAB6_CBDE

- The virtual environment was not created successfully because ensurepip is 
		not available.  On Debian/Ubuntu systems, you need to install the 
		python3-venv package using the following command.

    			sudo apt install python3.12-venv (Poner tu versión de python)
    			
    		- Ejecutar python3 -m venv venv 
    		
    		- Instalar sudo apt install python3.12-dev libpq-dev build-essential
    		
    		- Entrar entorno (source venv/bin/activate) y actualizar pip con: pip install --upgrade pip

			- Si no esta creado, crear o ir actualizando archivo requirements.txt: pip freeze > requirements.txt

			- pip install -r requirements.txt: Para instalar los requisitos


- Tener en cuenta que si se cambia valores de variables de entorno hay que hacer "exit" en el terminal para volver a cargar el entorno virtual y que se actualice.