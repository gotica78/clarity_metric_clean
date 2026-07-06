import glob
import os
import json

def load_all_posts(base_path="data/telegram"):
    all_posts = []
    
    # Ищем по реальной структуре: data/telegram/data/*/posts.json
    patterns = [
        os.path.join(base_path, "data", "*", "posts.json"),  
        os.path.join(base_path, "*", "posts.json"),             
        os.path.join("telegraphite", "data", "*", "posts.json"), 
    ]
    
    for pattern in patterns:
        for file in glob.glob(pattern):
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    posts = json.load(f)
                    if isinstance(posts, list):
                        all_posts.extend(posts)
                        print(f"✅ Загружено {len(posts)} постов из {file}")
            except Exception as e:
                print(f"⚠️ Ошибка {file}: {e}")
    
    print(f"📊 Всего загружено {len(all_posts)} постов")
    return all_posts