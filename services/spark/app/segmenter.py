import os

import numpy as np
from PIL import Image

try:
    import cv2
    import torch
    from torchvision import transforms
    import segmentation_models_pytorch as smp
except Exception:
    cv2 = None
    torch = None
    transforms = None
    smp = None


class ImageSegmentation:
    """
    Класс сегментации тайлов спутниковых снимков.

    Если модель или зависимости недоступны, класс работает в безопасном
    режиме и возвращает пустую маску-заглушку.
    """

    def __init__(self, weights_path="best_model.pth", device=None):
        self.weights_path = weights_path
        self.device = device or self._detect_device()
        self.device_warning = None
        self.ready = False
        self.model = None
        self.transforms = None
        self.fallback_reason = None

        if not self._can_use_real_model():
            return

        try:
            self.model = self._load_model(weights_path)
            self.transforms = self._get_transforms()
            self.ready = True
        except Exception as error:
            self.fallback_reason = f"Не удалось загрузить модель сегментации: {error}"

    def _detect_device(self):
        if torch is None:
            return "cpu"
        requested_device = os.getenv("SEGMENTATION_DEVICE", "auto").lower()
        if requested_device == "cpu":
            return "cpu"
        if requested_device.startswith("cuda"):
            if torch.cuda.is_available():
                return requested_device
            self.device_warning = (
                "В переменной SEGMENTATION_DEVICE запрошена CUDA, "
                "но PyTorch внутри контейнера пока не видит GPU. Используется CPU."
            )
            return "cpu"
        return "cuda" if torch.cuda.is_available() else "cpu"

    def _can_use_real_model(self):
        missing_modules = []
        if torch is None:
            missing_modules.append("torch")
        if transforms is None:
            missing_modules.append("torchvision")
        if smp is None:
            missing_modules.append("segmentation_models_pytorch")
        if cv2 is None:
            missing_modules.append("opencv-python-headless")

        if missing_modules:
            self.fallback_reason = (
                "Не найдены зависимости для сегментации: " + ", ".join(missing_modules)
            )
            return False

        if not os.path.exists(self.weights_path):
            self.fallback_reason = f"Файл весов не найден: {self.weights_path}"
            return False

        return True

    def _load_model(self, weights_path):
        model = smp.UnetPlusPlus(
            encoder_name="resnext50_32x4d",
            encoder_weights=None,
            in_channels=3,
            classes=1,
        )
        model.load_state_dict(torch.load(weights_path, map_location=self.device))
        model.to(self.device)
        model.eval()
        if self.device.startswith("cuda"):
            torch.backends.cudnn.benchmark = True
        return model

    def _get_transforms(self):
        return transforms.Compose(
            [
                transforms.Resize((224, 224)),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406],
                    std=[0.229, 0.224, 0.225],
                ),
            ]
        )

    def predict(self, image_path):
        if not self.ready:
            return self._empty_mask(image_path)

        image_tensor = self._preprocess_image(image_path)
        image_tensor = image_tensor.to(self.device, non_blocking=self.device.startswith("cuda"))

        with torch.inference_mode():
            if self.device.startswith("cuda"):
                with torch.cuda.amp.autocast():
                    output = self.model(image_tensor)
            else:
                output = self.model(image_tensor)
            mask = (torch.sigmoid(output) > 0.5).float().cpu().numpy()[0, 0]

        with Image.open(image_path) as image:
            width, height = image.size

        mask = cv2.resize(mask, (width, height), interpolation=cv2.INTER_NEAREST)
        return mask

    def _preprocess_image(self, image_path):
        image = Image.open(image_path).convert("RGB")
        return self.transforms(image).unsqueeze(0)

    def _empty_mask(self, image_path):
        with Image.open(image_path) as image:
            width, height = image.size
        return np.zeros((height, width), dtype=np.uint8)

    def save_prediction(self, mask, output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        mask_uint8 = (np.clip(mask, 0, 1) * 255).astype(np.uint8)

        if output_path.lower().endswith(".png"):
            rgba_mask = np.zeros((mask_uint8.shape[0], mask_uint8.shape[1], 4), dtype=np.uint8)
            rgba_mask[..., 0] = 255
            rgba_mask[..., 1] = 255
            rgba_mask[..., 2] = 255
            rgba_mask[..., 3] = mask_uint8
            Image.fromarray(rgba_mask, "RGBA").save(output_path, "PNG")
            return

        if cv2 is not None:
            cv2.imwrite(output_path, mask_uint8)
            return
        Image.fromarray(mask_uint8).save(output_path, "JPEG")
