
from PIL import Image
from PIL import ImageFilter
from PIL.ImageFilter import *
import os


def CreatePath(name):
    outPath = r"C:\Users\emili\Desktop\ClusterASD\Images\ImgFiltros/"+name+"Filtros/"

    os.makedirs(outPath, exist_ok=True)


def ImageFilter(path):

    for imagePath in os.listdir(path):

        inputPath = os.path.join(path, imagePath)
        simg = Image.open(inputPath)

        fullOutPath = os.path.join(path, imagePath)
        # fullOutPath contains the path of the output
        # image that needs to be generated
        simg.rotate(90).filter(ModeFilter(size=9)).save(fullOutPath)
        
        simg.close()
        print(f"Filtrada: {fullOutPath}", end = "\r")
    
    print('')
