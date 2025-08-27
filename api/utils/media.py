import os
from typing import Tuple

def ensure_under_2mb_jpeg(src_path: str, mime: str) -> Tuple[str, str]:
    try:
        if mime == "image/jpeg" and os.path.getsize(src_path) <= 2_000_000:
            return src_path, "image/jpeg"
    except Exception:
        pass

    try:
        from PIL import Image
    except Exception:
        return src_path, (mime or "image/jpeg")

    from PIL import Image
    im = Image.open(src_path).convert("RGB")
    out_path = src_path.rsplit(".", 1)[0] + ".jpg"

    for q in (90, 85, 80, 75, 70, 65, 60, 55, 50):
        im.save(out_path, format="JPEG", optimize=True, progressive=True, quality=q)
        if os.path.getsize(out_path) <= 2_000_000:
            return out_path, "image/jpeg"

    w, h = im.size
    for scale in (0.9, 0.8, 0.7, 0.6, 0.5):
        im_res = im.resize((max(1, int(w*scale)), max(1, int(h*scale))))
        im_res.save(out_path, format="JPEG", optimize=True, progressive=True, quality=70)
        if os.path.getsize(out_path) <= 2_000_000:
            return out_path, "image/jpeg"

    return out_path, "image/jpeg"
