from fdet import io, MTCNN

detector = MTCNN()
image = io.read_as_rgb('image.jpeg')

detections = detector.detect(image)
output_image = io.draw_detections(image, detections, color='white', thickness=5)
io.save('output.jpg', output_image)