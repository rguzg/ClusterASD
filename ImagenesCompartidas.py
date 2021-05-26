from typing import List

class ImagenesCompartidas:
    def __init__(self, img_range: List[int], nombre_folder: str) -> None:
        self.img_range = img_range
        self.imagenes = []
        self.nombre_folder = nombre_folder

        self.LeerImagenes()

    def LeerImagenes(self):
        for i in range(self.img_range[0], self.img_range[1] + 1):
            f = open(f'{self.nombre_folder}/{i}.jpg', 'rb')
            self.imagenes.append(f.read())
            f.close()

