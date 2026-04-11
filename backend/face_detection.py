import cv2
import fitz
import numpy as np
from typing import List
from backend.models import BoundingBox

def detect_faces_on_page(page: fitz.Page, page_number: int) -> List[BoundingBox]:
    """
    Renders the given fitz Page as a high-resolution image, converts it to grayscale, 
    and uses OpenCV's Haar cascades to detect unredacted frontal faces.
    Returns a list of BoundingBox objects mapped back to the original PDF coordinate space.
    """
    zoom: float = 2.0  # Scale up for better detail
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    
    # Convert PyMuPDF pixmap to numpy array
    img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    
    # Convert to Grayscale
    if pix.n >= 3:
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    else:
        gray = img_array
        
    # Load OpenCV defaults for Face cascades
    cascade_path = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
    face_cascade = cv2.CascadeClassifier(cascade_path)
    
    faces = face_cascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(30, 30)
    )
    
    bboxes: List[BoundingBox] = []
    for (x, y, w, h) in faces:
        # Step down by zoom factor to restore back to native PDF coordinates
        pdf_x0 = x / zoom
        pdf_y0 = y / zoom
        pdf_x1 = (x + w) / zoom
        pdf_y1 = (y + h) / zoom
        
        bboxes.append(BoundingBox(
            page_number=page_number,
            x0=pdf_x0,
            y0=pdf_y0,
            x1=pdf_x1,
            y1=pdf_y1
        ))
        
    return bboxes
