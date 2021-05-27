from Video import CreateVideo
from json import loads, dumps
from names.words import GetName
from typing import List, Optional
from ImagenesCompartidas  import ImagenesCompartidas

import socket
import Images
import math
import multiprocessing
import pickle

class Broker:
    def __init__(self, hostname: str, port: int) -> None:
        self.servidores_procesamiento = []

        self.socket = socket.socket()
        self.hostname = hostname
        self.port = port

    def listen(self) -> None:
        self.socket.bind((self.hostname, self.port))
        self.socket.listen()

        print("Servidor broker iniciado. Esperando nuevas conexiones.")

        while True:
            print(f"Número de servidores de procesamiento registrados: {len(self)}")

            conn, address = self.socket.accept()

            print(f"Nueva conexión de {address}")
            print("Manejando conexión")

            continue_listening = False
            decoded_json = b""
            extra_data = b""
            extra_data_size = 0

            while True:
                buffer = conn.recv(1024)
                recieved_bytes = len(buffer)

                if not buffer:
                    break
                else:
                    if(not decoded_json):
                        # El JSON nunca será mayor a 1024 bytes 
                        decoded_json = loads(buffer)

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
                            # para que el cliente empieze a esperar la respuesta
                            if(extra_data_size <= 0):
                                conn.send(b'1')
                                break

            if(isinstance(decoded_json, dict)):
                self.manejar_mensaje(decoded_json, extra_data, conn)
                conn.close()

    def manejar_mensaje(self, mensaje: dict, extra_data: bytes, socket: Optional[socket.socket] = None):
        switch_manejador = {
            "NODE_CONNECT": lambda puerto, _, socket: self.registrar_nodo(puerto, socket),
            "VIDEO": lambda _, video, socket: self.manejar_video(video, socket)
        }

        try:
            switch_manejador[mensaje['type']](mensaje['message'], extra_data, socket)
        except KeyError:
            print("Opción incorrecta en key: Type")
            socket.send(b'{"type": "END_ERROR", "message":"Opcion incorrecta en key: Type del JSON de la peticion"}')

    def registrar_nodo(self, puerto: int, socket: socket.socket):
        response_json = {}

        if(self.servidores_procesamiento.count(puerto) == 0):
            self.servidores_procesamiento.append(puerto)
            response_json = {"type": "NODE_CONNECTED", "message": puerto}

            print(f"Servidor procesamiento agregado: {puerto}")
        else:
            response_json = {"type": "NODE_EXISTS", "message": puerto}
            print(f"Ya existe un nodo con el puerto: {puerto}")

        socket.send(dumps(response_json).encode('ASCII'))

    def manejar_video(self, video, socket):
        if(len(self) == 0):
            print("No hay ningun servidor de procesamiento registrado")
            return

        video_name = GetName()

        video_file = open(f'Videos/{video_name}.mp4', 'wb')
        video_file.write(video)

        frames = Images.VideoToImage(f'Videos/{video_name}.mp4', video_name) - 1

        images_to_share = math.floor(frames / len(self))
        shared_images_counter = 0

        shared_images = []
        result_images = []

        process_pool = multiprocessing.Pool(len(self))
        processes = []

        available_servers = multiprocessing.Queue(len(self))

        for server in self.servidores_procesamiento:
            available_servers.put(server)

        for i in range(len(self)):
            if(i == len(self) - 1):
                shared_images_struct = ImagenesCompartidas([shared_images_counter + 1, frames], f'Images{video_name}')
            else:
                shared_images_struct = ImagenesCompartidas([shared_images_counter + 1, shared_images_counter + images_to_share], f'Images{video_name}')

            shared_images.append(shared_images_struct)

            shared_images_counter += images_to_share

        for struct in shared_images:
            servidor_asignado = available_servers.get()
            process = process_pool.apply_async(self.EnviarAProcesar, (struct, servidor_asignado))
            processes.append(process)

        for process in processes:
            try:
                result_structure = process.get(1000)

                if(not result_structure):
                   raise Exception('Los servidores no pudieron terminar de procesar las imagenes')

                if(isinstance(result_structure, ImagenesCompartidas)):
                    result_images.append(result_structure)
                else:
                   raise Exception('Los servidores no pudieron terminar de procesar las imagenes')
            except Exception as e:
                self.servidores_procesamiento.remove(e.args[0])
                print(f"El servidor {e} no está disponible")

                socket.send(b'{"type": "END_ERROR", "message": ""}')
                return

        for result in result_images:
            contador = 0
            for i in range(result.img_range[0], result.img_range[1] + 1):

                image = open(f'Images{video_name}/{i + 1}.jpg', 'wb')
                image.write(result.imagenes[contador])
                image.close()

                contador += 1

        CreateVideo(f'Images{video_name}')

        filtered_video = open(f'Videos/Images{video_name}video.mp4', 'rb').read()

        socket.send(b'{"type": "VIDEO_COMPLETE"}')
        socket.send(filtered_video)
                
    def EnviarAProcesar(self, imagenes_compartidas: ImagenesCompartidas, puerto_servidor: int):
        print(f"Enviando al servidor {puerto_servidor} para que procese imagenes del {imagenes_compartidas.img_range[0]} a {imagenes_compartidas.img_range[1]}")

        pickled_structure = pickle.dumps(imagenes_compartidas)
        client_socket = socket.socket()

        try:
            client_socket.connect(('localhost', puerto_servidor))
        except ConnectionRefusedError:
            raise ConnectionRefusedError(puerto_servidor)

        client_socket.send(b'{"type": "PROCESS", "message":"CONTINUE"}')
        should_continue = client_socket.recv(1024)

        if(should_continue):
            # Según el protocolo, antes de enviar pickled_structure, se debe de enviar que tan largo es pickled_structure
            client_socket.send(len(pickled_structure).to_bytes(8, 'little'))

            should_send = client_socket.recv(1024)

            if(should_send):
                    client_socket.send(pickled_structure)

                    # El servidor avisará cuando se deba de empezar a esperar la respuesta
                    should_continue = client_socket.recv(1024)

                    if(should_continue):
                        result_pickled_struct = b''

                        while True:
                            buffer = client_socket.recv(1024)

                            if not buffer:
                                break
                            else:
                                result_pickled_struct += buffer

                        return pickle.loads(result_pickled_struct)

        raise Exception(puerto_servidor, "El servidor no está listo para recibir. Intentalo de nuevo más tarde")
        

    def __len__(self):
        return len(self.servidores_procesamiento)

if __name__ == '__main__':
    b = Broker("localhost", 2000)
    b.listen()