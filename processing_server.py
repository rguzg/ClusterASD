import socket
import json
import sys
import pickle
from Filter import ImageFilter
from ImagenesCompartidas import ImagenesCompartidas

class Processing_Server():
    def __init__(self, address: str, port: int, broker_port: int = 2000, max_attempts: int = 3) -> None:
        self.socket_server = socket.socket()
        self.hasBroker = False
        self.address = address
        self.port = port
        self.broker_port = broker_port
        self.max_attempts = max_attempts
        self.current_images = 0

        if(self.port == self.broker_port):
            raise ValueError("El puerto asignado al servidor de procesamiento no puede ser el mismo que el del servidor broker")

    def listen(self) -> None:
        self.socket_server.bind((self.address, self.port))
        self.socket_server.listen()

        print(f"Servidor de procesamiento @ {self.address}:{self.port} iniciado")

        while True:
            conn, address = self.socket_server.accept()

            print(f"Nueva conexión de {address}")
            print("Manejando conexión")

            continue_listening = False
            decoded_json = b""
            extra_data = b""
            extra_data_size = 0

            while True:
                buffer = conn.recv(1024)
                recieved_bytes = len(buffer)

                if(not decoded_json):
                    # El JSON nunca será mayor a 1024 bytes 
                    decoded_json = json.loads(buffer)

                    continue_listening = True if decoded_json['message'] == 'CONTINUE' else False

                    if(continue_listening):
                        conn.send(b'1')
                    else:
                        break

                    continue
                if(continue_listening):
                    if(not extra_data_size):
                        extra_data_size = int.from_bytes(buffer, 'little')
                        conn.send(b'1')
                    else:
                        if(extra_data_size >= 0):
                            extra_data += buffer
                            extra_data_size -= recieved_bytes
                        # El protocolo debe avisar cuando haya terminado de leer el archivo enviado
                        # para que el broker mande la siguiente imagen
                        if(extra_data_size <= 0):                            
                            conn.send(b'1')
                            break

            if(isinstance(decoded_json, dict)):
                image_structure = pickle.loads(extra_data)
                self.manejar_imagenes(image_structure)

                return_structure = pickle.dumps(ImagenesCompartidas(image_structure.img_range, str(self.port)))

                conn.send(return_structure)

                conn.close()
        
    def manejar_imagenes(self, image_structure: ImagenesCompartidas):
        for i in range(len(image_structure.imagenes)):
            image = open(f'{self.port}/{i+1}.jpg', 'wb')
            image.write(image_structure.imagenes[i])
            image.close()

        ImageFilter(str(self.port))

    def searchBroker(self) -> bool:
        print(f"Buscando servidor broker @ localhost:{self.broker_port}")
        print(f"Se realizarán {self.max_attempts} intentos de conexión")

        attempts = 0
        socket_client = socket.socket()

        while (attempts < self.max_attempts):
            try:
                socket_client.connect(('localhost', self.broker_port))
                request_json = {"type": "NODE_CONNECT", "message": self.port}

                socket_client.send(json.dumps(request_json).encode('ASCII'))

                response = socket_client.recv(1024)
                
                try:
                    response = json.loads(response)

                    if(response['type'] == 'NODE_CONNECTED'):
                        socket_client.close()
                        return True
                    if(response['type'] == 'NODE_EXISTS'):
                        print(f"Ya existe un servidor de procesamiento con el puerto: {self.port}")
                        raise ValueError
                except:
                    continue
            except:
                print("Fallo al conectarse con el servidor broker")
                attempts += 1

        socket_client.close()
        return False

    def kill(self) -> None:
        self.socket_server.close()

if __name__ == '__main__':
    port = 0

    try:
        port = int(sys.argv[1])
    except:
        print("El número de puerto no es válido")

    s = Processing_Server('localhost', port)
    
    if(s.searchBroker()):
        print("Conectado al servidor broker")
        s.listen()
    else:
        print("Fallaron los intentos de conexión al broker")
        s.kill()
