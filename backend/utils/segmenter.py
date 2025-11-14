import os
import numpy as np
import torch
from PIL import Image
import cv2
from torchvision import transforms
import segmentation_models_pytorch as smp

class ImageSegmentation:
    def __init__(self, weights_path="best_model.pth", device=None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self._load_model(weights_path)
        self.transforms = self._get_transforms()

    def _load_model(self, weights_path):
        model = smp.UnetPlusPlus(
            encoder_name="resnext50_32x4d",
            encoder_weights=None,
            in_channels=3,
            classes=1
        )
        model.load_state_dict(torch.load(weights_path, map_location=self.device))
        model.to(self.device)
        model.eval()
        return model

    def _get_transforms(self):
        return transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

    def predict(self, image_path):
        image_tensor = self._preprocess_images(image_path)
        image_tensor = image_tensor.to(self.device)
        with torch.no_grad():
            output = self.model(image_tensor)
            mask = (torch.sigmoid(output) > 0.5).float().cpu().numpy()[0, 0]
            mask = cv2.resize(mask, (1000, 1000), interpolation=cv2.INTER_NEAREST)
        return mask

    def _preprocess_images(self, image_path):
        image = Image.open(image_path).convert("RGB")
        return self.transforms(image).unsqueeze(0)

    def save_prediction(self, mask, output_path):
        cv2.imwrite(output_path, (mask * 255).astype(np.uint8))